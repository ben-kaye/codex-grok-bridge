use agent_client_protocol::{
    PermissionOption, PermissionOptionKind, RequestPermissionOutcome, RequestPermissionResponse,
    SelectedPermissionOutcome, ToolKind,
};
use serde_json::{Value, json};

/// Translate an ACP `RequestPermissionRequest` into a Codex server request.
///
/// Returns `(method, params)` for the JSON-RPC request to send to the Codex client.
/// The method depends on the ACP ToolKind:
///   - `Execute` -> `item/commandExecution/requestApproval`
///   - `Edit`/`Delete`/`Move` -> `item/fileChange/requestApproval`
///   - Others -> `item/commandExecution/requestApproval` (fallback)
pub fn translate_permission_to_codex(
    request: &agent_client_protocol::RequestPermissionRequest,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
) -> (String, Value) {
    let kind = request.tool_call.fields.kind.unwrap_or(ToolKind::Other);

    let title = request
        .tool_call
        .fields
        .title
        .as_deref()
        .unwrap_or("unknown tool");

    match kind {
        ToolKind::Edit | ToolKind::Delete | ToolKind::Move => {
            let method = "item/fileChange/requestApproval".to_string();
            let params = json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "startedAtMs": time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
                "reason": title,
                "grantRoot": null,
            });
            (method, params)
        }
        _ => {
            // Default to command execution approval
            let method = "item/commandExecution/requestApproval".to_string();
            let params = json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "startedAtMs": time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
                "approvalId": null,
                "environmentId": null,
                "reason": title,
                "command": title,
                "cwd": null,
                "commandActions": [],
                "networkApprovalContext": null,
                "additionalPermissions": null,
                "proposedExecpolicyAmendment": null,
                "proposedNetworkPolicyAmendments": null,
                "availableDecisions": ["accept", "acceptForSession", "decline", "cancel"],
            });
            (method, params)
        }
    }
}

/// Translate a Codex approval response (JSON) into an ACP `RequestPermissionResponse`.
///
/// The Codex response contains a `decision` field with values like:
///   - `"accept"` / `"acceptForSession"` -> select the first "allow" option
///   - `"decline"` / `"cancel"` -> select the first "reject" option
pub fn translate_codex_approval_to_acp(
    response: &Value,
    options: &[PermissionOption],
) -> RequestPermissionResponse {
    let decision = response
        .get("decision")
        .and_then(|v| v.as_str())
        .unwrap_or("decline");

    let is_approve = matches!(
        decision,
        "accept" | "acceptForSession" | "acceptWithExecpolicyAmendment"
    );

    let outcome = find_matching_option(options, is_approve)
        .map(|opt| {
            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                opt.option_id.clone(),
            ))
        })
        .unwrap_or(RequestPermissionOutcome::Cancelled);

    RequestPermissionResponse::new(outcome)
}

/// Find the first permission option matching the desired approve/reject intent.
fn find_matching_option(options: &[PermissionOption], approve: bool) -> Option<&PermissionOption> {
    if approve {
        options.iter().find(|o| {
            matches!(
                o.kind,
                PermissionOptionKind::AllowOnce | PermissionOptionKind::AllowAlways
            )
        })
    } else {
        options.iter().find(|o| {
            matches!(
                o.kind,
                PermissionOptionKind::RejectOnce | PermissionOptionKind::RejectAlways
            )
        })
    }
}
