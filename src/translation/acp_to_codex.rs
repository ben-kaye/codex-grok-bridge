use std::path::{Path, PathBuf};

use agent_client_protocol::{
    ContentBlock, EmbeddedResourceResource, SessionUpdate, ToolCallStatus, ToolKind,
};
use serde_json::{Value, json};

use super::id_map::IdMap;
use super::state::{AgentMessageOutput, ToolDiffSet, TurnOutput};

/// Result of translating a session update.
pub struct TranslationResult {
    /// Codex notifications to emit: (method, params).
    pub notifications: Vec<(String, Value)>,
    /// Latest rendered diff collection for one ACP tool call.
    pub diff_update: Option<ToolDiffSet>,
}

/// Translate an ACP `SessionUpdate` into one or more Codex notification JSON values.
///
/// Each returned notification tuple is `(method, params)` for a Codex `ServerNotification`.
/// A single ACP update may produce multiple Codex notifications (e.g., a ToolCall
/// produces both an `item/started` and potentially content notifications).
///
/// The returned diff update replaces the previous collection for that ACP tool
/// call when building the turn-level aggregate.
pub fn translate_session_update(
    update: &SessionUpdate,
    thread_id: &str,
    turn_id: &str,
    agent_message_item_id: &str,
    id_map: &mut IdMap,
) -> TranslationResult {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => {
            let delta = extract_text_from_content(&chunk.content);
            TranslationResult {
                notifications: vec![(
                    "item/agentMessage/delta".to_string(),
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "itemId": agent_message_item_id,
                        "delta": delta,
                    }),
                )],
                diff_update: None,
            }
        }
        SessionUpdate::AgentThoughtChunk(chunk) => {
            let delta = extract_text_from_content(&chunk.content);
            TranslationResult {
                notifications: vec![(
                    "item/reasoning/summaryTextDelta".to_string(),
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "itemId": format!("{turn_id}-reasoning"),
                        "delta": delta,
                        "summaryIndex": 0,
                    }),
                )],
                diff_update: None,
            }
        }
        SessionUpdate::ToolCall(tool_call) => {
            let tool_call_id = tool_call.tool_call_id.0.as_ref();
            let item_id = id_map.create_item_for_tool(tool_call_id);
            let kind = effective_tool_kind(tool_call.kind, tool_call.meta.as_ref());
            id_map.set_tool_kind(tool_call_id, kind);
            id_map.set_tool_title(tool_call_id, tool_call.title.clone());

            // Determine the Codex item type based on ACP ToolKind
            let item = if is_file_change_kind(kind) {
                id_map.mark_file_change_item_started(tool_call_id);
                file_change_item(&item_id, &[], "inProgress")
            } else {
                // ACP is the transport, not an MCP source. Project ordinary
                // Grok work as native command activity so it does not create a
                // spurious entry in the Sources panel.
                command_execution_item(&item_id, &tool_call.title, "inProgress")
            };

            TranslationResult {
                notifications: vec![(
                    "item/started".to_string(),
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": item,
                    }),
                )],
                diff_update: None,
            }
        }
        SessionUpdate::ToolCallUpdate(update) => {
            let tool_call_id = update.tool_call_id.0.as_ref();
            let item_id = id_map
                .lookup_item(tool_call_id)
                .cloned()
                .unwrap_or_else(|| format!("unknown-{tool_call_id}"));

            let mut notifications = Vec::new();
            let mut rendered_diffs = Vec::new();
            let mut diff_collection_changed = false;

            if let Some(title) = &update.fields.title {
                id_map.set_tool_title(tool_call_id, title.clone());
            }
            if let Some(kind) = update.fields.kind {
                id_map.set_tool_kind(tool_call_id, kind);
            } else if let Some(kind) = grok_tool_kind(update.meta.as_ref()) {
                id_map.set_tool_kind(tool_call_id, kind);
            }

            let kind = id_map
                .lookup_tool_kind(tool_call_id)
                .copied()
                .unwrap_or(ToolKind::Other);
            let mut projected_changes = Vec::new();

            // Emit output delta if there's content
            if let Some(content) = &update.fields.content {
                for tc_content in content {
                    match tc_content {
                        agent_client_protocol::ToolCallContent::Content(c) => {
                            let text = extract_text_from_content(&c.content);
                            if !text.is_empty() {
                                // Route to correct notification based on tool kind
                                let method = if is_file_change_kind(kind) {
                                    "item/fileChange/outputDelta"
                                } else {
                                    "item/commandExecution/outputDelta"
                                };
                                notifications.push((
                                    method.to_string(),
                                    json!({
                                        "threadId": thread_id,
                                        "turnId": turn_id,
                                        "itemId": item_id,
                                        "delta": text,
                                    }),
                                ));
                            }
                        }
                        agent_client_protocol::ToolCallContent::Diff(d) => {
                            let (change, rendered) = project_diff(d, kind);
                            projected_changes.push(change);
                            rendered_diffs.push(rendered);
                        }
                        _ => {} // Terminal -- handled via ACP Client trait locally
                    }
                }
            }

            if !projected_changes.is_empty() {
                let changed = id_map.lookup_tool_file_changes(tool_call_id)
                    != Some(projected_changes.as_slice());
                diff_collection_changed = changed;
                id_map.set_tool_file_changes(tool_call_id, projected_changes.clone());

                if is_file_change_kind(kind) && !id_map.file_change_item_started(tool_call_id) {
                    notifications.insert(
                        0,
                        (
                            "item/started".to_string(),
                            json!({
                                "threadId": thread_id,
                                "turnId": turn_id,
                                "item": file_change_item(
                                    &item_id,
                                    &projected_changes,
                                    "inProgress",
                                ),
                            }),
                        ),
                    );
                    id_map.mark_file_change_item_started(tool_call_id);
                }

                // ACP collection fields replace previous values. Grok repeats
                // the same diff on completion, so only stream it when changed.
                if changed {
                    for rendered in &rendered_diffs {
                        notifications.push((
                            "item/fileChange/outputDelta".to_string(),
                            json!({
                                "threadId": thread_id,
                                "turnId": turn_id,
                                "itemId": item_id,
                                "delta": rendered,
                            }),
                        ));
                    }
                }
            } else if is_file_change_kind(kind) && !id_map.file_change_item_started(tool_call_id) {
                notifications.insert(
                    0,
                    (
                        "item/started".to_string(),
                        json!({
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "item": file_change_item(&item_id, &[], "inProgress"),
                        }),
                    ),
                );
                id_map.mark_file_change_item_started(tool_call_id);
            }

            // If the tool call is completed or failed, emit item/completed
            if let Some(status) = &update.fields.status
                && matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed)
            {
                let codex_status = match status {
                    ToolCallStatus::Completed => "completed",
                    ToolCallStatus::Failed => "failed",
                    _ => "inProgress",
                };

                let title = update
                    .fields
                    .title
                    .as_deref()
                    .or_else(|| id_map.lookup_tool_title(tool_call_id))
                    .unwrap_or("Grok tool");
                let item = if is_file_change_kind(kind) {
                    file_change_item(
                        &item_id,
                        id_map.lookup_tool_file_changes(tool_call_id).unwrap_or(&[]),
                        codex_status,
                    )
                } else {
                    command_execution_item(&item_id, title, codex_status)
                };

                notifications.push((
                    "item/completed".to_string(),
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": item,
                    }),
                ));
            }

            TranslationResult {
                notifications,
                diff_update: diff_collection_changed.then(|| ToolDiffSet {
                    tool_call_id: tool_call_id.to_string(),
                    diffs: rendered_diffs,
                }),
            }
        }
        SessionUpdate::Plan(plan) => {
            let steps: Vec<Value> = plan
                .entries
                .iter()
                .map(|entry| {
                    json!({
                        "step": entry.content,
                        "status": match entry.status {
                            agent_client_protocol::PlanEntryStatus::Pending => "pending",
                            agent_client_protocol::PlanEntryStatus::InProgress => "inProgress",
                            agent_client_protocol::PlanEntryStatus::Completed => "completed",
                            _ => "pending",
                        },
                    })
                })
                .collect();

            TranslationResult {
                notifications: vec![(
                    "turn/plan/updated".to_string(),
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "explanation": null,
                        "plan": steps,
                    }),
                )],
                diff_update: None,
            }
        }
        SessionUpdate::UserMessageChunk(_) => {
            // Echo of user input -- the Codex client already has it, so ignore.
            TranslationResult {
                notifications: vec![],
                diff_update: None,
            }
        }
        SessionUpdate::AvailableCommandsUpdate(update) => {
            let commands: Vec<Value> = update
                .available_commands
                .iter()
                .map(|cmd| {
                    json!({
                        "name": cmd.name,
                        "description": cmd.description,
                    })
                })
                .collect();
            TranslationResult {
                notifications: vec![(
                    "gateway/availableCommands/updated".to_string(),
                    json!({
                        "threadId": thread_id,
                        "commands": commands,
                    }),
                )],
                diff_update: None,
            }
        }
        SessionUpdate::CurrentModeUpdate(update) => TranslationResult {
            notifications: vec![(
                "thread/status/changed".to_string(),
                json!({
                    "threadId": thread_id,
                    "mode": update.current_mode_id.0.as_ref(),
                }),
            )],
            diff_update: None,
        },
        SessionUpdate::ConfigOptionUpdate(update) => {
            let options: Vec<Value> = update
                .config_options
                .iter()
                .map(|opt| {
                    json!({
                        "id": opt.id.0.as_ref(),
                        "name": opt.name,
                    })
                })
                .collect();
            TranslationResult {
                notifications: vec![(
                    "gateway/configOptions/updated".to_string(),
                    json!({
                        "threadId": thread_id,
                        "configOptions": options,
                    }),
                )],
                diff_update: None,
            }
        }
        // ── Unstable variants ──────────────────────────────────────────
        SessionUpdate::SessionInfoUpdate(info) => {
            let mut notifications = Vec::new();

            // If the agent updated the session title, emit thread/name/updated.
            if let agent_client_protocol::MaybeUndefined::Value(ref title) = info.title {
                notifications.push((
                    "thread/name/updated".to_string(),
                    json!({
                        "threadId": thread_id,
                        "threadName": title,
                    }),
                ));
            }

            TranslationResult {
                notifications,
                diff_update: None,
            }
        }
        SessionUpdate::UsageUpdate(usage) => {
            // Map ACP UsageUpdate → Codex thread/tokenUsage/updated.
            //
            // ACP provides `used` (tokens in context) and `size` (window size).
            // We approximate the Codex breakdown from the limited data available.
            TranslationResult {
                notifications: vec![(
                    "thread/tokenUsage/updated".to_string(),
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "tokenUsage": {
                            "total": {
                                "totalTokens": usage.used,
                                "inputTokens": usage.used,
                                "cachedInputTokens": 0,
                                "cacheWriteInputTokens": 0,
                                "outputTokens": 0,
                                "reasoningOutputTokens": 0,
                            },
                            "last": {
                                "totalTokens": usage.used,
                                "inputTokens": usage.used,
                                "cachedInputTokens": 0,
                                "cacheWriteInputTokens": 0,
                                "outputTokens": 0,
                                "reasoningOutputTokens": 0,
                            },
                            "modelContextWindow": usage.size,
                        },
                    }),
                )],
                diff_update: None,
            }
        }

        // Future-proof: any new variants added upstream are silently ignored.
        _ => TranslationResult {
            notifications: vec![],
            diff_update: None,
        },
    }
}

/// Translate an ACP update while preserving assistant-text/tool ordering.
///
/// ACP streams text as chunks without explicit message boundaries. A tool call
/// closes the current text segment; later text starts a fresh Codex message
/// item so the UI renders `text -> tool -> text` in protocol order.
pub fn translate_ordered_session_update(
    update: &SessionUpdate,
    thread_id: &str,
    turn_id: &str,
    id_map: &mut IdMap,
    output: &mut TurnOutput,
) -> TranslationResult {
    let mut leading = Vec::new();

    let agent_message_item_id = match update {
        SessionUpdate::AgentMessageChunk(_) => {
            let (message, started) = output.ensure_agent_message(turn_id);
            let item_id = message.item_id.clone();
            if started {
                leading.push(translate_agent_message_started(
                    thread_id, turn_id, &item_id,
                ));
            }
            item_id
        }
        SessionUpdate::ToolCall(_) => {
            if let Some(message) = output.take_agent_message() {
                leading.push(translate_agent_message_completed(
                    thread_id,
                    turn_id,
                    &message,
                    "commentary",
                ));
            }
            String::new()
        }
        _ => String::new(),
    };

    let mut translated =
        translate_session_update(update, thread_id, turn_id, &agent_message_item_id, id_map);
    for (method, params) in &translated.notifications {
        output.apply_notification(method, params);
    }
    leading.append(&mut translated.notifications);
    translated.notifications = leading;
    translated
}

/// Translate a completed ACP prompt response into a Codex `turn/completed` notification.
pub fn translate_prompt_response(
    stop_reason: &str,
    thread_id: &str,
    turn_id: &str,
    started_at_ms: i64,
) -> (String, Value) {
    let normalized = stop_reason.to_ascii_lowercase();
    let status = match normalized.as_str() {
        "end_turn" => "completed",
        "cancelled" => "interrupted",
        _ => "completed",
    };

    let completed_at_ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    let completed_at_ms = i64::try_from(completed_at_ms).unwrap_or(i64::MAX);
    let duration_ms = completed_at_ms.saturating_sub(started_at_ms);

    (
        "turn/completed".to_string(),
        json!({
            "threadId": thread_id,
                "turn": {
                    "id": turn_id,
                    "items": [],
                    "itemsView": "full",
                    "status": status,
                    "error": null,
                    "startedAt": started_at_ms / 1_000,
                    "completedAt": completed_at_ms / 1_000,
                    "durationMs": duration_ms,
            },
        }),
    )
}

pub fn translate_agent_message_started(
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
) -> (String, Value) {
    (
        "item/started".to_string(),
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "item": {
                "type": "agentMessage",
                "id": item_id,
                "text": "",
                "phase": null,
                "memoryCitation": null,
            },
        }),
    )
}

pub fn translate_agent_message_completed(
    thread_id: &str,
    turn_id: &str,
    message: &AgentMessageOutput,
    phase: &str,
) -> (String, Value) {
    (
        "item/completed".to_string(),
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "item": {
                "type": "agentMessage",
                "id": message.item_id,
                "text": message.text,
                "phase": phase,
                "memoryCitation": null,
            },
        }),
    )
}

/// Build authoritative completion notifications for the final streamed
/// assistant message segment and the turn-wide reasoning item.
pub fn translate_stream_completion(
    thread_id: &str,
    turn_id: &str,
    agent_message: Option<&AgentMessageOutput>,
    reasoning_summary: &str,
) -> Vec<(String, Value)> {
    let mut notifications = Vec::new();
    if let Some(message) = agent_message {
        notifications.push(translate_agent_message_completed(
            thread_id,
            turn_id,
            message,
            "final_answer",
        ));
    }
    notifications.push((
        "item/completed".to_string(),
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "item": {
                "type": "reasoning",
                "id": format!("{turn_id}-reasoning"),
                "summary": if reasoning_summary.is_empty() {
                    Vec::<String>::new()
                } else {
                    vec![reasoning_summary.to_string()]
                },
                "content": [],
            },
        }),
    ));
    notifications
}

/// Translate an ACP `Usage` (from `PromptResponse`) into a Codex
/// `thread/tokenUsage/updated` notification payload.
///
/// This provides more detailed token breakdown than the streaming `UsageUpdate`
/// because `PromptResponse.usage` has per-category counts.
pub fn translate_prompt_usage(
    usage: &agent_client_protocol::Usage,
    thread_id: &str,
    turn_id: &str,
) -> (String, Value) {
    (
        "thread/tokenUsage/updated".to_string(),
        json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "tokenUsage": {
                "total": {
                    "totalTokens": usage.total_tokens,
                    "inputTokens": usage.input_tokens,
                    "cachedInputTokens": usage.cached_read_tokens.unwrap_or(0),
                    "cacheWriteInputTokens": 0,
                    "outputTokens": usage.output_tokens,
                    "reasoningOutputTokens": usage.thought_tokens.unwrap_or(0),
                },
                "last": {
                    "totalTokens": usage.total_tokens,
                    "inputTokens": usage.input_tokens,
                    "cachedInputTokens": usage.cached_read_tokens.unwrap_or(0),
                    "cacheWriteInputTokens": 0,
                    "outputTokens": usage.output_tokens,
                    "reasoningOutputTokens": usage.thought_tokens.unwrap_or(0),
                },
                "modelContextWindow": null,
            },
        }),
    )
}

fn is_file_change_kind(kind: ToolKind) -> bool {
    matches!(kind, ToolKind::Edit | ToolKind::Delete | ToolKind::Move)
}

/// Grok Build sends its namespaced tool kind in the initial tool-call metadata
/// before it repeats the value in the stable ACP `kind` field. Reading only
/// this explicitly namespaced extension lets the first Codex item use the
/// correct native type without interpreting arbitrary ACP metadata.
fn effective_tool_kind(
    protocol_kind: ToolKind,
    meta: Option<&agent_client_protocol::Meta>,
) -> ToolKind {
    if protocol_kind == ToolKind::Other {
        grok_tool_kind(meta).unwrap_or(protocol_kind)
    } else {
        protocol_kind
    }
}

fn grok_tool_kind(meta: Option<&agent_client_protocol::Meta>) -> Option<ToolKind> {
    let kind = meta?
        .get("x.ai/tool")?
        .get("kind")?
        .as_str()?
        .to_ascii_lowercase();
    match kind.as_str() {
        "read" => Some(ToolKind::Read),
        "edit" => Some(ToolKind::Edit),
        "delete" => Some(ToolKind::Delete),
        "move" => Some(ToolKind::Move),
        "search" => Some(ToolKind::Search),
        "execute" => Some(ToolKind::Execute),
        "think" => Some(ToolKind::Think),
        "fetch" => Some(ToolKind::Fetch),
        _ => None,
    }
}

fn command_execution_item(item_id: &str, title: &str, status: &str) -> Value {
    json!({
        "type": "commandExecution",
        "id": item_id,
        "command": title,
        "cwd": ".",
        "processId": null,
        "status": status,
        "commandActions": [],
        "aggregatedOutput": null,
        "exitCode": null,
        "durationMs": null,
    })
}

fn file_change_item(item_id: &str, changes: &[Value], status: &str) -> Value {
    json!({
        "type": "fileChange",
        "id": item_id,
        "changes": changes,
        "status": status,
    })
}

/// Convert ACP's old/new representation to both the native Codex change entry
/// and the git-style text consumed by `turn/diff/updated`.
fn project_diff(d: &agent_client_protocol::Diff, tool_kind: ToolKind) -> (Value, String) {
    let old = d.old_text.as_deref().unwrap_or("");
    let new_text = &d.new_text;
    let normalized_path = normalize_diff_path(&d.path);
    let path = normalized_path.to_string_lossy();
    let label_path = path.strip_prefix('/').unwrap_or(&path);
    let unified = similar::TextDiff::from_lines(old, new_text)
        .unified_diff()
        .context_radius(3)
        .header(&format!("a/{label_path}"), &format!("b/{label_path}"))
        .to_string();
    let rendered = format!("diff --git a/{label_path} b/{label_path}\n{unified}");

    let (kind, diff) = match tool_kind {
        ToolKind::Delete => (json!({ "type": "delete" }), old.to_string()),
        ToolKind::Move => (
            json!({ "type": "update", "movePath": null }),
            unified.clone(),
        ),
        _ if d.old_text.is_none() => (json!({ "type": "add" }), new_text.clone()),
        _ => (
            json!({ "type": "update", "movePath": null }),
            unified.clone(),
        ),
    };

    (
        json!({
            "path": path,
            "kind": kind,
            "diff": diff,
        }),
        rendered,
    )
}

fn normalize_diff_path(path: &Path) -> PathBuf {
    if !path.is_absolute() {
        return path.to_path_buf();
    }
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    let Some(parent) = path.parent() else {
        return path.to_path_buf();
    };
    let Some(file_name) = path.file_name() else {
        return path.to_path_buf();
    };
    std::fs::canonicalize(parent)
        .map(|canonical_parent| canonical_parent.join(file_name))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// Extract a text representation from a ContentBlock.
///
/// Returns the text directly for text content, or a descriptive placeholder for
/// non-text content (images, audio, resources).
fn extract_text_from_content(content: &ContentBlock) -> String {
    match content {
        ContentBlock::Text(t) => t.text.clone(),
        ContentBlock::Image(img) => {
            if let Some(uri) = &img.uri {
                format!("[image: {uri}]")
            } else {
                format!("[image: {}]", img.mime_type)
            }
        }
        ContentBlock::Audio(_) => "[audio content]".to_string(),
        ContentBlock::Resource(res) => match &res.resource {
            EmbeddedResourceResource::TextResourceContents(t) => t.text.clone(),
            EmbeddedResourceResource::BlobResourceContents(b) => {
                format!("[blob resource: {}]", b.uri)
            }
            _ => "[resource]".to_string(),
        },
        ContentBlock::ResourceLink(link) => {
            format!("[resource: {} ({})]", link.name, link.uri)
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_prompt_remains_interrupted() {
        let (_, params) = translate_prompt_response("Cancelled", "thread", "turn", 1_000);
        assert_eq!(params["turn"]["status"], "interrupted");
        assert_eq!(params["turn"]["itemsView"], "full");
    }

    #[test]
    fn prompt_completion_preserves_elapsed_time() {
        let now_ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let started_at_ms = i64::try_from(now_ms).unwrap() - 2_000;
        let (_, params) = translate_prompt_response("EndTurn", "thread", "turn", started_at_ms);
        assert_eq!(params["turn"]["startedAt"], started_at_ms / 1_000);
        assert!(params["turn"]["durationMs"].as_i64().unwrap() >= 2_000);
    }

    #[test]
    fn stream_completion_preserves_final_message_and_reasoning() {
        let message = AgentMessageOutput {
            item_id: "turn-msg-0".to_string(),
            text: "final result".to_string(),
        };
        let notifications =
            translate_stream_completion("thread", "turn", Some(&message), "thinking trace");
        assert_eq!(notifications[0].1["item"]["text"], "final result");
        assert_eq!(notifications[0].1["item"]["phase"], "final_answer");
        assert_eq!(
            notifications[1].1["item"]["summary"],
            json!(["thinking trace"])
        );
    }

    #[test]
    fn ordinary_acp_tool_does_not_register_as_a_source() {
        let mut ids = IdMap::new();
        let update = SessionUpdate::ToolCall(agent_client_protocol::ToolCall::new(
            "tool-1",
            "Read workspace",
        ));
        let translated =
            translate_session_update(&update, "thread", "turn", "turn-msg-0", &mut ids);
        let item = &translated.notifications[0].1["item"];
        assert_eq!(item["type"], "commandExecution");
        assert!(item.get("server").is_none());
    }

    #[test]
    fn grok_edit_projects_to_native_file_change_with_authoritative_changes() {
        let mut ids = IdMap::new();
        let started: SessionUpdate = serde_json::from_value(json!({
            "sessionUpdate": "tool_call",
            "toolCallId": "tool-edit-1",
            "title": "search_replace",
            "rawInput": {
                "file_path": "/workspace/src/lib.rs",
                "old_string": "old();",
                "new_string": "new();"
            },
            "_meta": {
                "x.ai/tool": {
                    "version": 1,
                    "name": "search_replace",
                    "kind": "edit",
                    "namespace": "grok_build"
                }
            }
        }))
        .unwrap();

        let translated =
            translate_session_update(&started, "thread", "turn", "turn-msg-0", &mut ids);
        assert_eq!(translated.notifications[0].1["item"]["type"], "fileChange");

        let update_json = json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-edit-1",
            "kind": "edit",
            "title": "Edit `/workspace/src/lib.rs`",
            "content": [{
                "type": "diff",
                "path": "/workspace/src/lib.rs",
                "oldText": "old();",
                "newText": "new();"
            }]
        });
        let update: SessionUpdate = serde_json::from_value(update_json.clone()).unwrap();
        let translated =
            translate_session_update(&update, "thread", "turn", "turn-msg-0", &mut ids);
        assert!(
            translated
                .notifications
                .iter()
                .any(|(method, _)| method == "item/fileChange/outputDelta")
        );
        assert_eq!(translated.diff_update.as_ref().unwrap().diffs.len(), 1);
        assert!(
            translated.diff_update.as_ref().unwrap().diffs[0]
                .starts_with("diff --git a/workspace/src/lib.rs b/workspace/src/lib.rs")
        );

        let mut completion_json = update_json;
        completion_json["status"] = json!("completed");
        let completion: SessionUpdate = serde_json::from_value(completion_json).unwrap();
        let translated =
            translate_session_update(&completion, "thread", "turn", "turn-msg-0", &mut ids);

        assert!(
            !translated
                .notifications
                .iter()
                .any(|(method, _)| method == "item/fileChange/outputDelta")
        );
        assert!(translated.diff_update.is_none());
        let completed = translated
            .notifications
            .iter()
            .find(|(method, _)| method == "item/completed")
            .unwrap();
        let item = &completed.1["item"];
        assert_eq!(item["type"], "fileChange");
        assert_eq!(item["status"], "completed");
        assert_eq!(item["changes"][0]["path"], "/workspace/src/lib.rs");
        assert_eq!(item["changes"][0]["kind"]["type"], "update");
        assert!(
            item["changes"][0]["diff"]
                .as_str()
                .unwrap()
                .contains("new();")
        );
        serde_json::from_value::<codex_app_server_protocol::ThreadItem>(item.clone()).unwrap();
    }

    #[test]
    fn stable_kind_update_can_upgrade_a_generic_started_tool() {
        let mut ids = IdMap::new();
        let started = SessionUpdate::ToolCall(agent_client_protocol::ToolCall::new(
            "tool-edit-2",
            "search_replace",
        ));
        let translated =
            translate_session_update(&started, "thread", "turn", "turn-msg-0", &mut ids);
        assert_eq!(
            translated.notifications[0].1["item"]["type"],
            "commandExecution"
        );

        let update: SessionUpdate = serde_json::from_value(json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-edit-2",
            "kind": "edit",
            "content": [{
                "type": "diff",
                "path": "/workspace/a.txt",
                "oldText": "a",
                "newText": "b"
            }]
        }))
        .unwrap();
        let translated =
            translate_session_update(&update, "thread", "turn", "turn-msg-0", &mut ids);
        assert_eq!(translated.notifications[0].0, "item/started");
        assert_eq!(translated.notifications[0].1["item"]["type"], "fileChange");
    }

    #[test]
    fn interleaved_text_and_tool_calls_keep_stream_order() {
        let mut ids = IdMap::new();
        let mut output = TurnOutput::new(0);
        let updates = [
            SessionUpdate::AgentMessageChunk(agent_client_protocol::ContentChunk::new(
                ContentBlock::Text(agent_client_protocol::TextContent::new("before")),
            )),
            SessionUpdate::ToolCall(agent_client_protocol::ToolCall::new(
                "tool-1",
                "Read workspace",
            )),
            SessionUpdate::AgentMessageChunk(agent_client_protocol::ContentChunk::new(
                ContentBlock::Text(agent_client_protocol::TextContent::new("after")),
            )),
        ];

        let notifications: Vec<_> = updates
            .iter()
            .flat_map(|update| {
                translate_ordered_session_update(update, "thread", "turn", &mut ids, &mut output)
                    .notifications
            })
            .collect();

        let methods: Vec<_> = notifications
            .iter()
            .map(|(method, _)| method.as_str())
            .collect();
        assert_eq!(
            methods,
            [
                "item/started",
                "item/agentMessage/delta",
                "item/completed",
                "item/started",
                "item/started",
                "item/agentMessage/delta",
            ]
        );
        assert_eq!(notifications[0].1["item"]["id"], "turn-msg-0");
        assert_eq!(notifications[2].1["item"]["text"], "before");
        assert_eq!(notifications[2].1["item"]["phase"], "commentary");
        assert_eq!(notifications[3].1["item"]["type"], "commandExecution");
        assert_eq!(notifications[4].1["item"]["id"], "turn-msg-1");

        let final_message = output.take_agent_message().unwrap();
        let completion = translate_stream_completion("thread", "turn", Some(&final_message), "");
        assert_eq!(completion[0].1["item"]["id"], "turn-msg-1");
        assert_eq!(completion[0].1["item"]["text"], "after");
        assert_eq!(completion[0].1["item"]["phase"], "final_answer");
    }
}
