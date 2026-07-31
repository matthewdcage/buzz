//! Push completed Buzz channel turns into Honcho (Phase 2).
//!
//! Mirrors the Cursor connector's REST push pattern (`_post_session_messages_mapped`)
//! but is invoked by `buzz-acp` after a successful harness turn — fail-open,
//! never blocks session creation or the agent turn path.
//!
//! Workspace / peer scoping matches Phase 1's read path in `honcho_fetch.rs`:
//! `workspace_id = owner_pubkey_hex`, agent `peer_id = agent_pubkey_hex`,
//! human `peer_id = author_pubkey_hex`.

use crate::queue::{self, FlushBatch};
use uuid::Uuid;

const MAX_CONTENT_LEN: usize = 25_000;

/// Payload for one completed turn pushed to Honcho.
#[derive(Debug, Clone)]
pub struct HonchoTurnWrite {
    pub workspace_id: String,
    pub session_id: String,
    pub agent_peer_id: String,
    pub user_peer_id: String,
    pub user_content: String,
    pub agent_content: Option<String>,
    pub metadata: serde_json::Value,
}

/// Honcho session id for a Buzz channel (+ optional thread root).
pub fn honcho_session_id(channel_id: Uuid, thread_root: Option<&str>) -> String {
    match thread_root {
        Some(root) if !root.is_empty() => {
            let short = if root.len() >= 16 {
                &root[..16]
            } else {
                root
            };
            format!("buzz-{channel_id}-{short}")
        }
        _ => format!("buzz-{channel_id}"),
    }
}

/// Extract the human author's pubkey and message text from a prompt batch.
///
/// Prefers the first non-agent author with non-empty content. Returns `None`
/// when the batch is empty or every message is blank.
pub fn extract_user_turn(batch: &FlushBatch, agent_pubkey_hex: &str) -> Option<(String, String)> {
    let agent_pk = agent_pubkey_hex.to_ascii_lowercase();
    for be in &batch.events {
        let author = be.event.pubkey.to_hex().to_ascii_lowercase();
        if author == agent_pk {
            continue;
        }
        let content = be.event.content.trim();
        if !content.is_empty() {
            return Some((author, truncate(content, MAX_CONTENT_LEN)));
        }
    }
    batch.events.first().and_then(|be| {
        let content = be.event.content.trim();
        if content.is_empty() {
            return None;
        }
        Some((
            be.event.pubkey.to_hex().to_ascii_lowercase(),
            truncate(content, MAX_CONTENT_LEN),
        ))
    })
}

/// Optional thread root from the last batch event's reply tags.
pub fn thread_root_from_batch(batch: &FlushBatch) -> Option<String> {
    let last = batch.events.last()?;
    queue::parse_thread_tags(&last.event)
        .root_event_id
        .filter(|s| !s.is_empty())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

fn api_base(api_url: &str) -> String {
    api_url.trim_end_matches('/').to_string()
}

async fn ensure_workspace(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    workspace_id: &str,
) -> Result<(), String> {
    let url = format!("{base}/v3/workspaces");
    let mut req = client.post(&url).json(&serde_json::json!({ "id": workspace_id }));
    if !token.is_empty() {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("workspace create: {e}"))?;
    if resp.status().is_success() || resp.status().as_u16() == 409 {
        Ok(())
    } else {
        Err(format!("workspace create: status {}", resp.status()))
    }
}

async fn ensure_peer(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    workspace_id: &str,
    peer_id: &str,
) -> Result<(), String> {
    let url = format!("{base}/v3/workspaces/{workspace_id}/peers");
    let mut req = client.post(&url).json(&serde_json::json!({ "id": peer_id }));
    if !token.is_empty() {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.map_err(|e| format!("peer create: {e}"))?;
    if resp.status().is_success() || resp.status().as_u16() == 409 {
        Ok(())
    } else {
        Err(format!("peer create: status {}", resp.status()))
    }
}

async fn ensure_session(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    turn: &HonchoTurnWrite,
) -> Result<(), String> {
    let url = format!(
        "{base}/v3/workspaces/{}/sessions",
        turn.workspace_id
    );
    let body = serde_json::json!({
        "id": turn.session_id,
        "metadata": turn.metadata,
        "peers": {
            turn.user_peer_id.clone(): { "observe_me": true },
            turn.agent_peer_id.clone(): { "observe_others": true }
        }
    });
    let mut req = client.post(&url).json(&body);
    if !token.is_empty() {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("session create: {e}"))?;
    if resp.status().is_success() || resp.status().as_u16() == 409 {
        Ok(())
    } else {
        Err(format!("session create: status {}", resp.status()))
    }
}

async fn post_messages(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    turn: &HonchoTurnWrite,
) -> Result<(), String> {
    let url = format!(
        "{base}/v3/workspaces/{}/sessions/{}/messages",
        turn.workspace_id, turn.session_id
    );
    let mut messages = vec![serde_json::json!({
        "peer_id": turn.user_peer_id,
        "content": turn.user_content,
        "metadata": { "role": "user", "source": "buzz-acp" }
    })];
    if let Some(ref agent_text) = turn.agent_content {
        let trimmed = agent_text.trim();
        if !trimmed.is_empty() {
            messages.push(serde_json::json!({
                "peer_id": turn.agent_peer_id,
                "content": truncate(trimmed, MAX_CONTENT_LEN),
                "metadata": { "role": "assistant", "source": "buzz-acp" }
            }));
        }
    }
    let mut req = client
        .post(&url)
        .json(&serde_json::json!({ "messages": messages }));
    if !token.is_empty() {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("messages push: {e}"))?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("messages push: status {}", resp.status()))
    }
}

/// Best-effort push of one turn to Honcho. Logs warnings on failure.
pub async fn push_turn(
    client: &reqwest::Client,
    api_url: &str,
    auth_token: &str,
    turn: HonchoTurnWrite,
) {
    let base = api_base(api_url);
    if let Err(e) = ensure_workspace(client, &base, auth_token, &turn.workspace_id).await {
        tracing::warn!(target: "honcho::write", "{}", e);
        return;
    }
    for peer in [&turn.user_peer_id, &turn.agent_peer_id] {
        if let Err(e) =
            ensure_peer(client, &base, auth_token, &turn.workspace_id, peer).await
        {
            tracing::warn!(target: "honcho::write", "{}", e);
            return;
        }
    }
    if let Err(e) = ensure_session(client, &base, auth_token, &turn).await {
        tracing::warn!(target: "honcho::write", "{}", e);
        return;
    }
    if let Err(e) = post_messages(client, &base, auth_token, &turn).await {
        tracing::warn!(target: "honcho::write", "{}", e);
    } else {
        tracing::info!(
            target: "honcho::write",
            session = %turn.session_id,
            workspace = %turn.workspace_id,
            "pushed Buzz turn to Honcho"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::BatchEvent;
    use nostr::{EventBuilder, Kind, Keys, PublicKey};
    use std::time::Instant;

    fn make_batch(content: &str, author_keys: &Keys) -> FlushBatch {
        let event = EventBuilder::new(Kind::Custom(9), content)
            .sign_with_keys(author_keys)
            .unwrap();
        FlushBatch {
            channel_id: Uuid::new_v4(),
            events: vec![BatchEvent {
                event,
                prompt_tag: "mention".to_string(),
                received_at: Instant::now(),
            }],
            cancelled_events: vec![],
            cancel_reason: None,
        }
    }

    #[test]
    fn session_id_flat_and_threaded() {
        let cid = Uuid::parse_str("3dcb8d00-dcd6-4b92-a11d-007cf696fe5a").unwrap();
        assert_eq!(
            honcho_session_id(cid, None),
            "buzz-3dcb8d00-dcd6-4b92-a11d-007cf696fe5a"
        );
        assert_eq!(
            honcho_session_id(cid, Some("12790b86e22237b827d8358c54180e69f208ed89988b0a80711b31768125ad24")),
            "buzz-3dcb8d00-dcd6-4b92-a11d-007cf696fe5a-12790b86e22237b8"
        );
    }

    #[test]
    fn extract_user_turn_skips_agent_author() {
        let agent_keys = Keys::generate();
        let human_keys = Keys::generate();
        let agent_hex = agent_keys.public_key().to_hex().to_ascii_lowercase();
        let human_hex = human_keys.public_key().to_hex().to_ascii_lowercase();

        let mut batch = make_batch("@Agent hello", &human_keys);
        batch.events.push(BatchEvent {
            event: EventBuilder::new(Kind::Custom(9), "noise")
                .sign_with_keys(&agent_keys)
                .unwrap(),
            prompt_tag: "self".to_string(),
            received_at: Instant::now(),
        });

        let (pk, content) = extract_user_turn(&batch, &agent_hex).unwrap();
        assert_eq!(pk, human_hex);
        assert!(content.contains("hello"));
    }
}
