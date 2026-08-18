use std::path::Path;

use agent_client_protocol::{
    ClientCapabilities, ContentBlock, FileSystemCapability, ForkSessionRequest, InitializeRequest,
    LoadSessionRequest, NewSessionRequest, PromptRequest, ProtocolVersion, ResumeSessionRequest,
    SetSessionConfigOptionRequest, SetSessionModeRequest, SetSessionModelRequest, TextContent,
};
use serde_json::Value;

use super::content::codex_input_to_acp;

/// Translate a Codex `initialize` request (as JSON params) into an ACP `InitializeRequest`.
///
/// The Codex initialize carries `clientInfo { name, version }` and capabilities.
/// We map these to the ACP equivalent. Filesystem and terminal callbacks remain
/// disabled so Grok owns its native file and command side effects.
pub fn translate_initialize(params: &Value) -> InitializeRequest {
    let mut req = InitializeRequest::new(ProtocolVersion::LATEST);

    if let Some(client_info) = params.get("clientInfo") {
        let name = client_info
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("codex");
        let version = client_info
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("0.0.0");
        req = req.client_info(agent_client_protocol::Implementation::new(name, version));
    }

    // Grok Build owns its native file and command side effects and projects
    // them as ACP tool updates. Client callbacks would move execution into the
    // bridge without adding a Codex-visible lifecycle.
    req = req.client_capabilities(
        ClientCapabilities::new()
            .fs(FileSystemCapability::new()
                .read_text_file(false)
                .write_text_file(false))
            .terminal(false),
    );

    req
}

/// Translate a Codex `thread/start` request (as JSON params) into an ACP `NewSessionRequest`.
///
/// The main field we need is `cwd` (working directory). If not provided, use the
/// given fallback.
pub fn translate_thread_start(params: &Value, fallback_cwd: &Path) -> NewSessionRequest {
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| fallback_cwd.to_path_buf());

    NewSessionRequest::new(cwd)
}

/// Translate a Codex `turn/start` request (as JSON params) into an ACP `PromptRequest`.
///
/// Extracts the user input array and converts it to ACP ContentBlocks.
pub fn translate_turn_start(params: &Value, session_id: &str) -> PromptRequest {
    let prompt = if let Some(inputs) = params.get("input").and_then(|v| v.as_array()) {
        codex_input_to_acp(inputs)
    } else {
        // Fallback: if there's no structured input, check for a plain text field
        vec![ContentBlock::Text(TextContent::new("[empty turn input]"))]
    };

    PromptRequest::new(session_id.to_string(), prompt)
}

/// Translate a Codex `turn/steer` request into Grok Build's send-now prompt.
///
/// `sendNow` is an XAI-specific ACP metadata extension. It asks Grok to replace
/// the running foreground prompt without cancelling session-owned background
/// work or queued prompts.
pub fn translate_turn_steer(params: &Value, session_id: &str) -> PromptRequest {
    let mut request = translate_turn_start(params, session_id);
    request
        .meta
        .get_or_insert_default()
        .insert("sendNow".to_string(), Value::Bool(true));
    request
}

/// Translate a Codex `thread/resume` request into an ACP `LoadSessionRequest`.
pub fn translate_thread_resume(
    params: &Value,
    session_id: &str,
    fallback_cwd: &Path,
) -> LoadSessionRequest {
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| fallback_cwd.to_path_buf());

    LoadSessionRequest::new(session_id.to_string(), cwd)
}

/// Translate a Codex `thread/resume` into an ACP `ResumeSessionRequest` (unstable).
///
/// Unlike `LoadSessionRequest`, `ResumeSessionRequest` does not replay message
/// history to the client — the agent picks up where it left off.
pub fn translate_thread_resume_session(
    params: &Value,
    session_id: &str,
    fallback_cwd: &Path,
) -> ResumeSessionRequest {
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| fallback_cwd.to_path_buf());

    ResumeSessionRequest::new(session_id.to_string(), cwd)
}

/// Translate a Codex `thread/fork` into an ACP `ForkSessionRequest` (unstable).
///
/// Delegates forking to the agent so it preserves its internal conversation
/// context, rather than the gateway copying rollout items + creating a new session.
pub fn translate_thread_fork_session(
    params: &Value,
    session_id: &str,
    fallback_cwd: &Path,
) -> ForkSessionRequest {
    let cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| fallback_cwd.to_path_buf());

    ForkSessionRequest::new(session_id.to_string(), cwd)
}

/// Create an ACP `SetSessionModelRequest` (unstable).
///
/// Called when a Codex `thread/start` or `thread/resume` includes a `model`
/// field and the agent advertises model selection support.
pub fn translate_set_session_model(session_id: &str, model_id: &str) -> SetSessionModelRequest {
    SetSessionModelRequest::new(session_id.to_string(), model_id.to_string())
}

/// Translate into an ACP `SetSessionModeRequest`.
pub fn translate_set_session_mode(session_id: &str, mode_id: &str) -> SetSessionModeRequest {
    SetSessionModeRequest::new(session_id.to_string(), mode_id.to_string())
}

/// Translate into an ACP `SetSessionConfigOptionRequest`.
pub fn translate_set_session_config_option(
    session_id: &str,
    config_id: &str,
    value: &str,
) -> SetSessionConfigOptionRequest {
    SetSessionConfigOptionRequest::new(
        session_id.to_string(),
        config_id.to_string(),
        value.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialization_keeps_file_and_terminal_execution_in_grok() {
        let request = translate_initialize(&serde_json::json!({
            "clientInfo": { "name": "codex", "version": "0.148.0" }
        }));
        let capabilities = request.client_capabilities;
        assert!(!capabilities.fs.read_text_file);
        assert!(!capabilities.fs.write_text_file);
        assert!(!capabilities.terminal);
    }

    #[test]
    fn steering_uses_grok_send_now_metadata() {
        let request = translate_turn_steer(
            &serde_json::json!({
                "input": [{"type": "text", "text": "new direction"}]
            }),
            "session",
        );

        assert_eq!(request.session_id.0.as_ref(), "session");
        assert_eq!(request.meta.as_ref().unwrap()["sendNow"], true);
        assert_eq!(
            serde_json::to_value(request).unwrap()["_meta"]["sendNow"],
            true
        );
    }
}
