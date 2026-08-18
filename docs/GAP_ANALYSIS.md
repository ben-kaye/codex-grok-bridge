# Protocol Gap Analysis

> Audit of codex-acp-gateway coverage against both the Codex app-server
> protocol and the Agent Client Protocol (ACP). Last updated: 2026-02-23.

## Summary

| Area | Implemented | Gap | Notes |
|------|-------------|-----|-------|
| Codex client requests (v2) | 28 of 36 | 8 | Account, remote skills, steer/review |
| Codex server requests | 4 of 5 | 1 | Only token refresh remaining |
| Codex server notifications | 20 of ~35 | ~15 | Account, config, terminal notifications |
| ACP Agent trait (stable) | 8 of 8 | 0 | Full coverage |
| ACP Agent trait (unstable) | 4 of 4 | 0 | Full coverage (feature-gated) |
| ACP Client trait | 11 of 11 | 0 | Full coverage |
| ACP SessionUpdate (stable) | 9 of 9 | 0 | Full coverage |
| ACP SessionUpdate (unstable) | 2 of 2 | 0 | Full coverage (feature-gated) |

---

## 1. Codex Client Requests — Not Implemented

| Wire method | Priority | Notes |
|---|---|---|
| `turn/steer` | Medium | No ACP equivalent currently |
| `review/start` | Low | Codex-specific feature |
| `skills/remote/list` | Low | No ACP equivalent |
| `skills/remote/export` | Low | No ACP equivalent |
| `skills/config/write` | Low | No ACP equivalent |
| `app/list` | Low | No ACP equivalent |
| `account/read` | Low | Codex auth, not applicable |
| `account/login/start` | Low | Codex-specific |
| `account/login/cancel` | Low | Codex-specific |
| `account/logout` | Low | Codex-specific |
| `account/rateLimits/read` | Low | Codex-specific |
| `feedback/upload` | Low | Codex-specific |
| `mcpServer/oauth/login` | Low | Codex-specific |
| `windowsSandbox/setupStart` | Low | Windows-only |

### Experimental

| Wire method | Notes |
|---|---|
| `collaborationMode/list` | Experimental collaboration modes |
| `fuzzyFileSearch/session*` | Experimental incremental file search |

---

## 2. Codex Server Requests

### Implemented

| Wire method | ACP trigger | Notes |
|---|---|---|
| `item/commandExecution/requestApproval` | `request_permission` (ToolKind::Execute) | Stable approval flow |
| `item/fileChange/requestApproval` | `request_permission` (ToolKind::Edit/Delete/Move) | Stable approval flow |
| `item/tool/call` | `ext_method("tool/call", ...)` | Dynamic tool call — forwarded to Codex client |
| `item/tool/requestUserInput` | `ext_method("request_user_input", ...)` | Free-form user input — forwarded to Codex client |

### Not Implemented

| Wire method | Priority | Notes |
|---|---|---|
| `account/chatgptAuthTokens/refresh` | Low | OpenAI-specific auth token refresh |

---

## 3. Codex Server Notifications — Not Implemented

| Wire method | Priority | Notes |
|---|---|---|
| `thread/compacted` | Low | Context compaction notification |
| `item/plan/delta` | Low | Incremental plan text delta (we use `turn/plan/updated` instead) |
| `item/commandExecution/terminalInteraction` | Low | Terminal interaction events |
| `item/mcpToolCall/progress` | Low | MCP tool call progress |
| `item/reasoning/summaryPartAdded` | Low | Reasoning summary part boundaries |
| `item/reasoning/textDelta` | Low | Raw reasoning text (vs summary) |
| `rawResponseItem/completed` | Low | Raw model response items |
| `model/rerouted` | Low | Model reroute notification |
| `account/updated` | Low | Account state changes |
| `account/rateLimits/updated` | Low | Rate limit changes |
| `account/login/completed` | Low | Login completion |
| `app/list/updated` | Low | App list changes |
| `mcpServer/oauthLogin/completed` | Low | MCP OAuth completion |
| `deprecationNotice` | Low | Deprecation warnings |
| `configWarning` | Low | Config warnings |
| `fuzzyFileSearch/session*` | Low | File search results |
| `windowsSandbox/setupCompleted` | Low | Windows sandbox |
| `windows/worldWritableWarning` | Low | Windows security |

---

## 4. ACP Unstable Methods — Implemented

All ACP unstable agent methods are now implemented behind `features = ["unstable"]`.
The gateway checks agent capabilities at runtime and falls back gracefully.

| ACP method | Feature flag | Codex mapping | Notes |
|---|---|---|---|
| `session/set_model` | `unstable_session_model` | `thread/start`, `thread/resume` model param | Best-effort model selection after session creation |
| `session/list` | `unstable_session_list` | `thread/list` supplement | Agent sessions appended to local rollout listing |
| `session/fork` | `unstable_session_fork` | `thread/fork` delegation | Uses agent's fork when supported; fallback to local copy |
| `session/resume` | `unstable_session_resume` | `thread/resume` optimization | Skips history replay; fallback to `session/load` |

---

## 5. ACP Unstable SessionUpdate Variants — Implemented

| SessionUpdate variant | Feature flag | Codex notification | Notes |
|---|---|---|---|
| `SessionInfoUpdate` | `unstable_session_info_update` | `thread/name/updated` | Agent-provided title persisted to session index |
| `UsageUpdate` | `unstable_session_usage` | `thread/tokenUsage/updated` | Mid-turn streaming + final `PromptResponse.usage` |

---

## 6. ACP Extension Methods — Implemented

The gateway uses ACP's `ext_method` escape hatch to bridge features that have
no dedicated ACP protocol method but are needed by the Codex client.

| ACP `ext_method` name | Codex server request | Response mapping |
|---|---|---|
| `tool/call` | `item/tool/call` | `{ contentItems, success }` passed through |
| `request_user_input` | `item/tool/requestUserInput` | `{ answers: { qid: { answers } } }` passed through |

Unknown extension methods receive a `method_not_found` error response.

---

## 7. Translation Quality Issues

### Permission ToolKind bucketing is coarse

The approval translation maps everything that isn't Edit/Delete/Move to
`item/commandExecution/requestApproval`. ToolKind variants like `Read`,
`Browser`, `Notebook`, `Other` all get the same command-execution treatment.
Codex only has two approval types today, so this may not be fixable without
upstream changes.

### Hardcoded response fields

`handle_thread_start` hardcodes `approvalPolicy: "on-request"` and
`sandbox: { type: "dangerFullAccess" }`. These should reflect the agent's
actual capabilities or be configurable.

---

## 8. Recommended Next Steps

1. **`turn/steer`** — Mid-turn steering; no ACP equivalent yet
