use std::sync::Arc;

use serde_json::{Value, json};

/// Well-known ACP extension method names that the gateway translates into
/// Codex server requests.
pub const EXT_TOOL_CALL: &str = "tool/call";
pub const EXT_REQUEST_USER_INPUT: &str = "request_user_input";

/// Translate an ACP `ext_method("tool/call", ...)` into a Codex
/// `item/tool/call` server request.
///
/// The ACP params are expected to contain:
///   `{ "tool": "<tool_name>", "arguments": { ... } }`
///
/// Returns `(method, params)` ready to be sent as a Codex server request.
pub fn translate_ext_tool_call(
    params: &Value,
    thread_id: &str,
    turn_id: &str,
    call_id: &str,
) -> (String, Value) {
    let tool = params
        .get("tool")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    (
        "item/tool/call".to_string(),
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "callId": call_id,
            "tool": tool,
            "arguments": arguments,
        }),
    )
}

/// Translate a Codex `DynamicToolCallResponse` JSON back into an ACP
/// `ExtResponse` payload.
///
/// The Codex response has `{ "contentItems": [...], "success": bool }`.
/// We pass it through as-is since the ACP agent will interpret it.
pub fn translate_tool_call_response(response: &Value) -> Arc<serde_json::value::RawValue> {
    let result = json!({
        "contentItems": response.get("contentItems").cloned().unwrap_or(json!([])),
        "success": response.get("success").and_then(|v| v.as_bool()).unwrap_or(false),
    });
    serde_json::value::RawValue::from_string(result.to_string())
        .expect("valid JSON")
        .into()
}

/// Translate an ACP `ext_method("request_user_input", ...)` into a Codex
/// `item/tool/requestUserInput` server request.
///
/// The ACP params are expected to contain:
///   `{ "questions": [{ "id": "...", "header": "...", "question": "...", ... }] }`
///
/// Returns `(method, params)` ready to be sent as a Codex server request.
pub fn translate_ext_request_user_input(
    params: &Value,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
) -> (String, Value) {
    let questions = params.get("questions").cloned().unwrap_or(json!([]));

    (
        "item/tool/requestUserInput".to_string(),
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "itemId": item_id,
            "questions": questions,
        }),
    )
}

/// Translate a Codex `ToolRequestUserInputResponse` JSON back into an ACP
/// `ExtResponse` payload.
///
/// The Codex response has `{ "answers": { "<qid>": { "answers": ["..."] } } }`.
/// We pass it through as-is.
pub fn translate_user_input_response(response: &Value) -> Arc<serde_json::value::RawValue> {
    let result = json!({
        "answers": response.get("answers").cloned().unwrap_or(json!({})),
    });
    serde_json::value::RawValue::from_string(result.to_string())
        .expect("valid JSON")
        .into()
}
