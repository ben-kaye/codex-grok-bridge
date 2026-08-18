# First-class Grok MVP plan

Status: MVP implemented; upstream steering alignment pending. The ACP
translation layer is adapted from the pinned Apache-2.0
`codex-acp-gateway` implementation; this project owns the hybrid routing,
model namespacing, persistence, version gate, and desktop launcher.

1. Pin desktop-to-app-server and ACP handshakes, schemas, versions, and failure
   fixtures.
2. Add an app-server executable shim that launches the bundled Codex binary,
   removes its own and the launcher's overrides from the child environment,
   and proxies native Codex JSONL without semantic translation.
3. Add the launcher and prove that existing Codex models, task creation,
   streaming, approvals, cancellation, history, and resume still work. Native
   task traffic, including creation, stays byte-for-byte transparent and does
   not create bridge routing records; prove a Sol task remains usable after a
   vanilla Codex launch.
4. Initialize `grok agent stdio`, namespace its advertised models, and merge
   them into `model/list` with stable pagination and exact effort metadata.
5. Route `thread/start` by selected model, pin the backend, and persist only the
   Grok task ID to ACP session ID mapping plus routing metadata. Native Codex
   task IDs remain owned by native Codex.
6. Map Grok task create, read, list, resume, archive, and turn lifecycle into
   app-server responses and notifications.
7. Map Grok text, reasoning, plans, tools, usage, permissions, cancellation,
   and terminal errors without re-executing tool activity.
8. Merge Codex and Grok task listings with stable ordering and composite
   pagination. Reject cross-backend model changes explicitly.
9. Verify, in one desktop launch, a native Codex task and a Grok task, one Grok
   tool approval, concurrent streaming, cancellation, restart, listing, and
   resume for both backends.

Post-MVP protocol hardening: keep delta content and timing in authoritative
completion items, project internal ACP tools without registering an MCP source,
and segment assistant text around tool calls so item ordering matches the ACP
stream. Use monotonic request IDs, turn-correlated lifecycle transitions,
immediate stdio EOF shutdown, and per-backend composite list cursors. Project
Grok edit updates into native file-change items by reading its namespaced
initial tool kind, treating stable ACP updates as authoritative, replacing
repeated content collections, and preserving separate replacement hunks. ACP
filesystem callbacks remain disabled until they can provide the same approval
and item lifecycle without introducing a second writer. Keep ACP terminal
callbacks disabled so Grok retains its native command execution, permission,
and process lifecycle; pin both disabled capabilities in the initialization
fixture.

Replace the bridge-owned steering sequence with Grok Build's native send-now
path:

1. Translate `turn/steer` into another `session/prompt` for the active session
   with a unique prompt ID and `_meta.sendNow: true`; do not send
   `session/cancel`.
2. Start that prompt while its predecessor is still outstanding. Keep both ACP
   prompt completions correlated to the same Codex turn.
3. Suppress the predecessor's send-now cancellation. Only the newest prompt
   completion may complete the Codex turn.
4. Preserve multiple rapid steers in Grok's FIFO instead of overwriting one
   bridge-side pending value.
5. Prove with focused fixtures that the wire metadata is present, no explicit
   cancel is sent, stale completion cannot end the turn, and the supported
   Grok Build version retains background tasks, subagents, and queued work.
6. Keep `x.ai/interject` separate for non-cancelling, safe-point follow-ups.

Each step lands with one focused protocol test. Do not start the next step while
the current mapping has silent fallbacks or changes native Codex behavior.
