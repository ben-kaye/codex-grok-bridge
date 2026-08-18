# First-class Grok

## Outcome

Launch the Codex desktop UI once, choose either a native Codex model or Grok
4.6, and retain the selected runtime's native task behavior.

## Acceptance criteria

- Native Codex models and Grok 4.6 appear in the same model picker.
- Each model advertises only the reasoning efforts and input modalities its
  backend supports.
- Selecting a native Codex model preserves native task, history, approval, and
  streaming behavior through transparent app-server proxying.
- A task created with an OpenAI model, including Sol, is stored only by native
  Codex and remains readable and resumable after launching vanilla Codex
  without the bridge.
- Users can create, cancel, resume, and list Grok-backed tasks.
- A Grok task has at most one active turn; interrupts and completions apply only
  when their turn ID matches that active turn.
- Text, reasoning, plans, tool activity, usage, and errors stream incrementally.
- Interleaved assistant text and tool activity retain their ACP stream order;
  text resumed after a tool call is a new message item after that tool.
- Streamed reasoning and final text remain visible after completion, with the
  turn's elapsed time preserved.
- ACP remains an internal transport detail and does not appear as a chat source.
- Steering an active Grok turn uses Grok Build's send-now prompt path without
  a separate `session/cancel`: a concurrent `session/prompt` carries
  `_meta.sendNow: true`, the cancelled predecessor does not complete the Codex
  turn, and Grok-owned background tasks, subagents, and queued work survive.
- Grok's non-cancelling `x.ai/interject` extension remains distinct from
  user-facing `turn/steer`; it is reserved for follow-ups that should join the
  running prompt at Grok's next safe point.
- Permission requests are decided in the Codex UI and returned to Grok.
- Grok remains the sole terminal executor. ACP command results render through
  Grok's tool updates; the bridge does not advertise terminal callbacks.
- Grok remains the sole file-edit executor. Its ACP edit updates render as
  native Codex file-change items with concrete path, kind, and diff data.
- Repeated ACP completion content does not duplicate a diff, and separate
  edits to the same file remain visible in the turn-level diff.
- Codex-backed and Grok-backed tasks can run concurrently.
- Composite task-list cursors retain both backend cursors, including beyond a
  backend's first 1,000 tasks.
- Closing the stdio client terminates the bridge and both child runtimes.
- Task identity and backend ownership survive bridge restarts.
- Attempting to change an existing task to the other backend fails clearly.
- Unsupported Codex-only features are hidden or fail clearly.

## Not in the first release

- Patching the desktop bundle.
- Moving an existing task between Codex and Grok backends.
- Translating Grok into an OpenAI Responses model provider.
- Reimplementing Grok tools, authentication, plugins, or session storage.

## Compatibility boundary

- Codex desktop app-server: `codex-cli 0.148.x`.
- Grok Build ACP endpoint: `grok 1.0.x`, ACP protocol version 1.
- Grok 4.6 is advertised as `grok/grok-4.6` with text input and
  `low`, `medium`, `high`, and `xhigh` reasoning efforts.
- ACP filesystem and terminal callbacks are not advertised in the MVP; Grok
  Build uses its native tools and reports their effects through ACP session
  updates.
