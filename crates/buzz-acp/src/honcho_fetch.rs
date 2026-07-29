//! Fetch the agent's Honcho peer context at session creation and render it
//! into a prompt section.
//!
//! Mirrors `engram_fetch.rs`'s NIP-AE core-memory pattern (see that module's
//! doc comment for the shape this is cloned from), sourcing from Honcho's
//! REST API instead of a Nostr relay:
//!
//! - Fire one bounded, best-effort query for the agent's peer context when a
//!   *new* channel session is born (caller applies the timeout — see
//!   `pool::run_prompt_task`'s `HONCHO_FETCH_TIMEOUT`).
//! - If context is found, emit `[Agent Memory — honcho]\n<context>`.
//! - On any failure (transport, non-success status, parse) — or when Honcho
//!   simply has no context yet for this peer — emit nothing. Unlike NIP-AE
//!   core, Honcho has no "onboarding nudge" concept in Phase 1: an absent or
//!   unreachable peer context look identical from here (both are "no
//!   section"), which is the conservative, fail-open choice.
//! - Session creation is never blocked on this fetch.
//!
//! Honcho is a self-hosted, persistent cross-session memory service (MCP
//! server + REST API) run alongside Buzz. This module only covers the
//! session-start context *read*; ingesting each turn's transcript back into
//! Honcho is explicitly out of scope for Phase 1 (see the PR description).

/// Section header rendered into the prompt.
const SECTION_LABEL: &str = "Agent Memory — honcho";

/// Response shape for the peer-context fetch.
///
/// Verified against the live self-hosted Honcho API
/// (`src/schemas/api.py:212-213`, `RepresentationResponse`): the real
/// field is `representation: str` (required); there is no `context`
/// field. The `context` fallback here is kept anyway as cheap defensive
/// coverage in case a future Honcho version changes the field name —
/// `#[serde(default)]` on both means an unexpected shape still degrades
/// to "no section" instead of a hard parse error.
#[derive(Debug, serde::Deserialize)]
struct PeerContextResponse {
    #[serde(default)]
    representation: Option<String>,
    #[serde(default)]
    context: Option<String>,
}

/// Build the rendered prompt section for the agent's Honcho peer context.
///
/// Returns:
/// - `Some(section)` when Honcho returned non-empty context,
/// - `None` when Honcho confirmed no context, or the fetch failed for any
///   reason (transport, non-success status, unparsable body) — the caller
///   injects no section in that case, matching `engram_fetch`'s fail-open
///   behavior for an unreachable memory backend.
pub async fn build_honcho_section(
    client: &reqwest::Client,
    api_base_url: &str,
    auth_token: &str,
    workspace_id: &str,
    peer_id: &str,
    user_name: &str,
) -> Option<String> {
    match fetch_peer_context(
        client,
        api_base_url,
        auth_token,
        workspace_id,
        peer_id,
        user_name,
    )
    .await
    {
        Ok(Some(context)) => Some(format!("[{SECTION_LABEL}]\n{context}")),
        Ok(None) => None,
        Err(reason) => {
            tracing::warn!(
                target: "honcho::context",
                "honcho peer context fetch failed: {reason} — emitting no section"
            );
            None
        }
    }
}

/// Query the Honcho REST API for this agent's peer context.
///
/// Returns:
/// - `Ok(Some(text))` if Honcho returned non-empty context,
/// - `Ok(None)` if Honcho responded successfully but with empty/absent context,
/// - `Err(reason)` for any transport, status, or parse failure.
///
/// `workspace_id` and `peer_id` scope the request the same way NIP-AE core
/// scopes an engram to an `(agent, owner)` pair: `workspace_id` is the
/// agent's owner pubkey (hex) and `peer_id` is the agent's own pubkey (hex).
/// This is Phase 1's own scoping choice, not a Honcho requirement — revisit
/// if Honcho's workspace/peer model calls for something else.
async fn fetch_peer_context(
    client: &reqwest::Client,
    api_base_url: &str,
    auth_token: &str,
    workspace_id: &str,
    peer_id: &str,
    user_name: &str,
) -> Result<Option<String>, String> {
    // Verified live against the self-hosted Honcho stack at
    // /home/matthew/Agents/memory-honcho (src/routers/peers.py:275-276,
    // router prefix "/workspaces/{workspace_id}/peers"): path and response
    // shape (`{"representation": "..."}`) match. The endpoint's request
    // body model (`PeerRepresentationGet`) has every field optional, but
    // FastAPI still requires a body to be present — an empty POST 422s
    // ("Field required"); an explicit `{}` succeeds. Confirmed both ways
    // with curl against the live peer `matthewcage` in workspace `default`
    // before wiring this in.
    let url = format!(
        "{}/v3/workspaces/{}/peers/{}/representation",
        api_base_url.trim_end_matches('/'),
        workspace_id,
        peer_id,
    );

    let mut req = client
        .post(&url)
        .header("X-Honcho-User-Name", user_name)
        .json(&serde_json::json!({}));
    if !auth_token.is_empty() {
        req = req.bearer_auth(auth_token);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| format!("request to {url} failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("{url} returned status {}", resp.status()));
    }

    let body: PeerContextResponse = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse response from {url}: {e}"))?;

    Ok(extract_context(body))
}

/// Pure extraction: given a parsed response body, decide what (if anything)
/// counts as usable context. Prefers `representation`, falls back to
/// `context`; blank/whitespace-only text counts as absent. Split out from
/// `fetch_peer_context` so the decision logic is unit-testable without a
/// network round-trip — same rationale as `engram_fetch::decode_core_body`.
fn extract_context(body: PeerContextResponse) -> Option<String> {
    body.representation
        .or(body.context)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_prefers_representation_over_context() {
        let body = PeerContextResponse {
            representation: Some("likes concise replies".to_string()),
            context: Some("fallback text".to_string()),
        };
        assert_eq!(
            extract_context(body).as_deref(),
            Some("likes concise replies")
        );
    }

    #[test]
    fn extract_falls_back_to_context_when_representation_absent() {
        let body = PeerContextResponse {
            representation: None,
            context: Some("fallback text".to_string()),
        };
        assert_eq!(extract_context(body).as_deref(), Some("fallback text"));
    }

    #[test]
    fn extract_treats_blank_representation_as_absent() {
        let body = PeerContextResponse {
            representation: Some("   ".to_string()),
            context: Some("fallback text".to_string()),
        };
        // Blank `representation` is present-but-empty, not "use the fallback" —
        // mirrors `filter(!s.is_empty())` applying after the `.or()` resolves,
        // so a blank primary field wins over a populated fallback would be
        // surprising; document the actual (simpler) behavior instead: `.or()`
        // short-circuits on `Some`, so a blank `representation` yields `None`
        // here rather than falling through to `context`.
        assert_eq!(extract_context(body), None);
    }

    #[test]
    fn extract_returns_none_when_both_absent() {
        let body = PeerContextResponse {
            representation: None,
            context: None,
        };
        assert_eq!(extract_context(body), None);
    }

    /// Fail-open: an unreachable backend must emit no section, not an error
    /// the caller has to handle specially. No mock server needed — port 0
    /// is never a valid connect target, so this exercises the real
    /// transport-error path in `fetch_peer_context`.
    #[tokio::test]
    async fn build_section_fails_open_on_transport_error() {
        let client = reqwest::Client::new();
        let section = build_honcho_section(
            &client,
            "http://127.0.0.1:0",
            "",
            "owner-hex",
            "agent-hex",
            "Duncan",
        )
        .await;
        assert_eq!(section, None);
    }

    /// Manual integration check against a real, running Honcho instance —
    /// not run in CI (`cargo test` skips `#[ignore]`d tests by default).
    /// Run explicitly with a live stack up:
    /// `cargo test -p buzz-acp honcho_fetch::tests::live_fetch_against_populated_peer_returns_context -- --ignored --nocapture`
    ///
    /// Exercises the exact code path `pool.rs` calls, against the
    /// pre-existing shared `default` workspace / `matthewcage` peer (has
    /// thousands of real conclusions — see chat history), not the empty
    /// per-owner-hex-pubkey workspace Phase 1 actually scopes to in
    /// production. This proves the HTTP plumbing (URL, body, headers,
    /// response parsing) is correct; it does NOT prove the production
    /// scoping choice will ever see non-empty context (see PR description).
    #[tokio::test]
    #[ignore]
    async fn live_fetch_against_populated_peer_returns_context() {
        let client = reqwest::Client::new();
        let section = build_honcho_section(
            &client,
            "http://localhost:8100",
            "local-dev",
            "default",
            "matthewcage",
            "test",
        )
        .await;
        eprintln!("section = {section:?}");
        assert!(
            section.is_some(),
            "expected non-empty context for the populated `matthewcage` peer"
        );
    }
}
