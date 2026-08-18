# First-class Grok MVP plan

1. Pin desktop-to-app-server and ACP handshakes, schemas, versions, and failure
   fixtures.
2. Add an app-server executable shim that launches the bundled Codex binary,
   removes its own override from the child environment, and proxies native
   Codex JSONL without semantic translation.
3. Add the launcher and prove that existing Codex models, task creation,
   streaming, approvals, cancellation, history, and resume still work.
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

Each step lands with one focused protocol test. Do not start the next step while
the current mapping has silent fallbacks or changes native Codex behavior.
