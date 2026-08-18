# Architecture

> The lower-level translation sections are inherited from the pinned reverse
> adapter. The hybrid topology and ownership rules below are project-specific.

## Hybrid topology

The desktop starts `codex-grok-bridge` through `CODEX_CLI_PATH`. The bridge
spawns two app-server-compatible peers: the bundled Codex app-server and its
own reverse-adapter mode backed by `grok agent stdio`. `model/list`,
`thread/list`, and initialization are aggregated. All other requests are
routed by model selection or thread ownership. Only Grok ownership is recorded;
an unknown thread is native by default. Native task JSONL, including
`thread/start`, passes byte-for-byte in both directions, so OpenAI-model
rollouts remain vanilla Codex rollouts. Grok model IDs are namespaced as
`grok/*` only on the Codex-facing side.

Native server requests retain their IDs. Grok server requests receive a
connection-local ID namespace so simultaneous permission prompts cannot
collide. The selected child alone owns tool execution and filesystem effects.

## Overview

codex-acp-gateway is a protocol translation gateway and WebSocket proxy for
ACP (Agent Client Protocol) agents. It operates in two modes:

- **Codex mode**: Translates between the Codex app-server protocol and ACP,
  enabling any Codex client (VS Code extension, CLI, web) to use any
  ACP-compliant agent as its backend.
- **ACP proxy mode**: Exposes any stdio-based ACP agent over WebSocket with no
  protocol translation — just framing conversion so remote ACP clients can
  reach a local agent subprocess over the network.

## Protocol comparison

| Aspect                | Codex app-server protocol                    | ACP (Agent Client Protocol)               |
|-----------------------|----------------------------------------------|--------------------------------------------|
| **Transport**         | stdio NDJSON or WebSocket                    | stdio NDJSON                               |
| **Wire format**       | JSON-RPC (no `"jsonrpc"` field)              | JSON-RPC 2.0 (`"jsonrpc": "2.0"`)         |
| **Session concept**   | Thread (thread/start, thread/resume)         | Session (session/new, session/load)        |
| **Prompt concept**    | Turn (turn/start)                            | Prompt (session/prompt)                    |
| **Streaming**         | Server notifications (item/* family)         | session/update notifications               |
| **Approval flow**     | Server requests (item/*/requestApproval)     | Client request (session/requestPermission) |
| **Cancellation**      | Client request (turn/interrupt) with response| Client notification (session/cancel)       |
| **File operations**   | Handled by codex-core internally             | Native agent tools or optional client callbacks |
| **Terminal ops**      | Handled by codex-core internally             | Native Grok tools; client callbacks disabled |
| **Request IDs**       | String or Integer, independent space         | Integer, independent space                 |

## Data flow

```
                          codex-acp-gateway
                    +--------------------------+
                    |                          |
  Codex Client      |   +------------------+   |      ACP Agent
  (stdio or WS)     |   |                  |   |   (stdin/stdout)
       |            |   |   Translation    |   |        |
       |   Codex    |   |     Engine       |   |  ACP   |
       |  NDJSON    |   |                  |   | NDJSON  |
       v            |   +--------+---------+   |         v
  +---------+       |            |             |   +---------+
  |         | read  |   +--------v---------+   |   |         |
  | Codex   |------>|   |  Request ID      |   |   |  ACP    |
  |Transport|       |   |  Correlation Map |   |   | Process |
  |  Layer  |<------|   +------------------+   |   | Manager |
  |         | write |            |             |   |         |
  +---------+       |   +--------v---------+   |   +---------+
       ^            |   | ACP Client       |   |        ^
       |            |   | - permissions    |   |        |
       |            |   | - session updates|   |        |
       |            |   +------------------+   |        |
       |            |                          |        |
       |            +--------------------------+        |
       |                                                |
       +--- stdio or WebSocket (Codex) ------------------+
                                         stdin/stdout (ACP agent subprocess)
```

## Request lifecycle

### Initialization

```
Codex Client                Gateway                    ACP Agent
    |                          |                          |
    |-- initialize ----------->|                          |
    |   {id, method, params}   |-- initialize ----------->|
    |   (no "jsonrpc")         |   {jsonrpc, id, method}  |
    |                          |                          |
    |                          |<- initialize response ---|
    |<- initialize response ---|   {jsonrpc, id, result}  |
    |   {id, result}           |                          |
```

### Thread/Session creation

```
Codex Client                Gateway                    ACP Agent
    |                          |                          |
    |-- thread/start --------->|                          |
    |   ThreadStartParams      |-- session/new ---------->|
    |                          |   NewSessionRequest      |
    |                          |                          |
    |                          |<- session/new response --|
    |<- thread/start response -|   NewSessionResponse     |
    |   ThreadStartResponse    |                          |
    |                          |                          |
    |<- thread/started --------|  (gateway synthesizes)   |
    |   notification           |                          |
```

### Turn/Prompt with streaming

```
Codex Client                Gateway                    ACP Agent
    |                          |                          |
    |-- turn/start ----------->|                          |
    |   TurnStartParams        |-- session/prompt ------->|
    |                          |   PromptRequest          |
    |                          |                          |
    |                          |<- session/update --------|
    |                          |   AgentMessageChunk      |
    |<- item/agentMessage/delta|                          |
    |                          |                          |
    |                          |<- session/update --------|
    |                          |   ToolCall               |
    |<- item/started -----------|                          |
    |<- item/completed ---------|                          |
    |                          |                          |
    |                          |<-- session/requestPermission --|
    |<- item/commandExecution/ |                          |
    |   requestApproval ------>|                          |
    |   (user approves)        |-- permission response -->|
    |                          |                          |
    |                          |<- session/prompt resp ---|
    |<- turn/completed --------|   PromptResponse         |
    |   notification           |                          |
```

### Active-turn steering

Grok Build 1.0.5 exposes two XAI-specific controls above standard ACP. They
have different semantics and must not be treated as interchangeable:

| Control | XAI wire shape | Grok behavior | Bridge use |
|---------|----------------|---------------|------------|
| Send now | another `session/prompt` with `_meta.sendNow: true` | Queues the new prompt, cancels the foreground turn, and preserves background tasks, subagents, and queued work | Target mapping for Codex `turn/steer` |
| Interject | `ext_method("x.ai/interject", {sessionId, text, ...})` | Adds a user message to the running prompt at Grok's next safe point without cancelling it | Reserved for non-interrupting follow-ups |
| Interrupt | `session/cancel` | Explicitly cancels the active prompt; absent metadata, Grok also cancels subagents | Mapping for Codex `turn/interrupt` only |

The official Grok client implements user-facing send-now by issuing the second
prompt immediately; the Grok session actor owns queue insertion and
cancellation as one operation. The bridge's current implementation instead
sends `session/cancel`, stores one pending steer, waits for the old prompt to
finish, and then sends another prompt. That fallback works at the Codex UI
level but differs from Grok's native behavior: it can cancel subagents, records
an explicit user cancellation, and cannot preserve more than one rapid steer.

The intended mapping is:

1. Validate `threadId` and `expectedTurnId` against the active Codex turn.
2. Send a new ACP prompt immediately with a unique prompt ID and
   `_meta.sendNow: true`.
3. Keep the original and replacement prompt futures associated with the same
   Codex turn.
4. Ignore the predecessor's `Cancelled` completion; the newest prompt alone
   emits `turn/completed`.

`x.ai/interject` is intentionally not the default `turn/steer` mapping. Grok
drains an interjection before a model call, after a tool batch, or just before
turn completion. It does not interrupt an in-progress model stream, making it
appropriate for plan-review or permission follow-ups rather than user-facing
send-now behavior.

These controls are not part of portable ACP. Compatibility is tied to the
pinned Grok Build range and must be checked by protocol fixtures. Upstream
references:

- [`x.ai/interject` handler](https://github.com/xai-org/grok-build/blob/d71f6e0c1f5acc5469e503e192fe14824e6f8c90/crates/codegen/xai-grok-shell/src/extensions/interject.rs)
- [Grok client send-now request](https://github.com/xai-org/grok-build/blob/d71f6e0c1f5acc5469e503e192fe14824e6f8c90/crates/codegen/xai-grok-pager/src/app/effects/mod.rs)
- [Grok prompt queue decision](https://github.com/xai-org/grok-build/blob/d71f6e0c1f5acc5469e503e192fe14824e6f8c90/crates/codegen/xai-grok-shell/src/session/acp_session_impl/prompt_queue.rs)
- [Grok send-now cancellation](https://github.com/xai-org/grok-build/blob/d71f6e0c1f5acc5469e503e192fe14824e6f8c90/crates/codegen/xai-grok-shell/src/session/acp_session_impl/tasks_cancel.rs)

### ACP side-effect ownership

The Grok MVP advertises neither filesystem nor terminal callbacks. Grok Build
owns edits and command execution through its native tools, then reports their
effects through `session/update` tool updates. This keeps approval, process
lifecycle, and each side effect in one runtime. The bridge's client
implementation handles only permissions, session updates, and supported
extension methods.

## Module responsibilities

### `src/main.rs`
Entry point. Parses CLI arguments, initializes tracing, sets up the tokio
runtime with a `LocalSet` (required for ACP's !Send futures), and orchestrates
the gateway lifecycle. Routes to codex mode or ACP proxy mode based on `--mode`.

### `src/lib.rs`
Module declarations and session orchestration. Contains `run_session()` — the
core event loop that `select!`s on both the Codex transport channel and ACP
events, dispatching to the translation engine.

### `src/config.rs`
Defines the CLI interface using clap. Key options:
- `--mode`: `codex` (default) or `acp-proxy`
- `--listen`: transport URL — `stdio://` (default) or `ws://IP:PORT`
- `--agent-cmd`: path/name of the ACP agent binary
- `--cwd`: working directory for the agent subprocess
- Remaining args after `--`: forwarded to the agent process

### `src/error.rs`
Gateway-specific error types using thiserror. Covers transport errors, protocol
translation errors, and subprocess management failures.

### `src/acp_proxy.rs`
ACP proxy mode implementation. Accepts WebSocket connections and bridges each
one to its own ACP agent subprocess via stdin/stdout NDJSON. No protocol
translation — just framing conversion (WebSocket Text frames <-> newline-delimited
JSON lines).

### `src/command_exec.rs`
Sandbox-aware subprocess execution for Codex `command/exec` requests. Wraps
commands through the sandbox policy before spawning.

### `src/sandbox.rs`
Bubblewrap (`bwrap`) sandbox integration. Provides `wrap_command()` which
translates a `SandboxPolicy` into bwrap bind-mount and namespace flags.

### `src/transport/`
Codex-side transport layer (pluggable: stdio or WebSocket):
- **stdio**: Reads NDJSON lines from stdin, writes NDJSON to stdout
- **websocket**: Reads/writes individual WebSocket Text frames (one JSON per frame)
- **outgoing**: Transport-agnostic `OutgoingMessageSender` wrapping `mpsc::Sender<String>`
- Both transports produce `TransportEvent` and consume `String` via the same channel interface

### `src/acp/`
ACP subprocess management and Client trait implementation:
- **spawn**: Spawns the ACP agent binary, manages its stdin/stdout/stderr
- **client_impl**: Implements the ACP `Client` trait to handle callbacks:
  - `request_permission` -> forwards to Codex client as approval request
  - `session_notification` -> translates and forwards to Codex as item/* notifications
- **fs_handler**: dormant `fs/read_text_file` and `fs/write_text_file`
  implementations; filesystem capabilities are not advertised to Grok in the MVP

### `src/translation/`
Bidirectional protocol translation engine:
- **codex_to_acp**: Translates incoming Codex requests to ACP method calls
  - `initialize` params mapping (ClientInfo <-> Implementation)
  - `thread/start` -> `session/new` (ThreadStartParams -> NewSessionRequest)
  - `turn/start` -> `session/prompt` (TurnStartParams + InputItem[] -> PromptRequest + ContentBlock[])
  - `turn/interrupt` -> `session/cancel`
- **acp_to_codex**: Translates ACP events to Codex notifications
  - `SessionUpdate::AgentMessageChunk` -> `item/agentMessage/delta`
  - `SessionUpdate::ToolCall` -> `item/started` + tool-specific notifications
  - `SessionUpdate::ToolCallUpdate` -> delta notifications + `item/completed`
  - `SessionUpdate::Plan` -> `item/plan/delta`
  - `PromptResponse` (stop_reason) -> `turn/completed`
- **approval**: Maps between ACP `RequestPermissionRequest` and Codex
  `CommandExecutionRequestApprovalParams` / `FileChangeRequestApprovalParams`
- **ext_method**: Handles ACP extension methods (`tool/call`, `request_user_input`)
- **content**: Content block translation (InputItem <-> ContentBlock)
- **id_map**: Bidirectional map of Codex request IDs <-> ACP request IDs
- **state**: Translation state machine tracking

### `src/rollout/`
Thread history persistence and listing:
- **recorder**: Writes turn events to per-thread rollout files
- **list**: Paginated listing of recorded threads with cursor-based pagination
- **session_index**: Manages the thread/session index file

## Key types from each protocol

### Codex (codex-app-server-protocol)

```
JSONRPCMessage (untagged)
  |-- JSONRPCRequest    { id, method, params }
  |-- JSONRPCNotification { method, params }
  |-- JSONRPCResponse   { id, result }
  |-- JSONRPCError      { id, error: { code, message, data } }

ClientRequest (tagged by "method")
  |-- Initialize        { params: InitializeParams }
  |-- ThreadStart       { params: ThreadStartParams }        "thread/start"
  |-- TurnStart         { params: TurnStartParams }          "turn/start"
  |-- TurnInterrupt     { params: TurnInterruptParams }      "turn/interrupt"
  ...

ServerNotification (tagged by "method")
  |-- ThreadStarted     (ThreadStartedNotification)          "thread/started"
  |-- TurnStarted       (TurnStartedNotification)            "turn/started"
  |-- TurnCompleted     (TurnCompletedNotification)          "turn/completed"
  |-- ItemStarted       (ItemStartedNotification)            "item/started"
  |-- ItemCompleted     (ItemCompletedNotification)          "item/completed"
  |-- AgentMessageDelta (AgentMessageDeltaNotification)      "item/agentMessage/delta"
  ...

ServerRequest (tagged by "method")
  |-- CommandExecutionRequestApproval  "item/commandExecution/requestApproval"
  |-- FileChangeRequestApproval        "item/fileChange/requestApproval"
  ...
```

### ACP (agent-client-protocol)

```
Agent trait (methods the agent implements)
  |-- initialize(InitializeRequest) -> InitializeResponse
  |-- new_session(NewSessionRequest) -> NewSessionResponse
  |-- prompt(PromptRequest) -> PromptResponse
  |-- cancel(CancelNotification)
  |-- load_session(LoadSessionRequest) -> LoadSessionResponse
  |-- set_session_mode(SetSessionModeRequest) -> SetSessionModeResponse
  ...

Client trait (methods the client/gateway implements)
  |-- request_permission(RequestPermissionRequest) -> RequestPermissionResponse
  |-- session_notification(SessionNotification)      [session/update]
  |-- read_text_file(ReadTextFileRequest) -> ReadTextFileResponse
  |-- write_text_file(WriteTextFileRequest) -> WriteTextFileResponse

SessionUpdate (notification variants)
  |-- UserMessageChunk
  |-- AgentMessageChunk
  |-- AgentThoughtChunk
  |-- ToolCall
  |-- ToolCallUpdate
  |-- Plan
  |-- AvailableCommandsUpdate
  |-- CurrentModeUpdate
  |-- ConfigOptionUpdate
```

## Task/channel topology

### Stdio mode

```
tokio::task::LocalSet (run_until)
  |
  |-- [tokio::spawn] Codex Stdio Reader
  |     Reads stdin line-by-line, parses NDJSON -> mpsc -> event loop
  |
  |-- [tokio::spawn] Codex Stdio Writer
  |     mpsc -> serialize + write to stdout
  |
  |-- run_session (directly awaited)
        |-- ACP Connection (spawn_local tasks)
        |-- Translation Event Loop (select! on both channels)
```

### WebSocket mode

```
tokio::task::LocalSet (run_until)
  |
  |-- TCP Accept Loop (directly awaited)
        |
        per connection:
          |-- [tokio::spawn] WS Reader Task
          |     WebSocket Text frames -> parse JSON -> mpsc -> session
          |
          |-- [tokio::spawn] WS Writer Task
          |     mpsc -> WebSocket Text frames (+ Pong replies)
          |
          |-- [spawn_local] run_session
                |-- ACP Connection (spawn_local tasks, own subprocess)
                |-- Translation Event Loop (select! on both channels)
```

## Translation state machine

```
                    +------------------+
                    |   Disconnected   |
                    +--------+---------+
                             |
                     initialize request
                             |
                    +--------v---------+
                    |   Initializing   |
                    | (waiting for ACP |
                    |  init response)  |
                    +--------+---------+
                             |
                     init response received
                             |
                    +--------v---------+
                    |     Ready        |
                    | (no active       |
                    |  thread/session) |
                    +--------+---------+
                             |
                     thread/start -> session/new
                             |
                    +--------v---------+
                    |  Session Active  |
                    | (thread started, |
                    |  no active turn) |
                    +--------+---------+
                             |
                     turn/start -> session/prompt
                             |
                    +--------v---------+
              +---->|  Turn Active     |<----+
              |     | (streaming item  |     |
              |     |  notifications)  |     |
              |     +--------+---------+     |
              |              |               |
     session/update    prompt response    turn/interrupt
     notifications     (stop_reason)     -> session/cancel
              |              |               |
              |     +--------v---------+     |
              +-----|  Session Active  |-----+
                    | (turn complete,  |
                    |  ready for next) |
                    +------------------+
```

## Error handling strategy

- **Transport errors** (broken pipe, malformed JSON): Log and terminate the gateway
- **Translation errors** (unknown method, missing field): Return JSON-RPC error to the originating side
- **ACP subprocess crash**: Detect via process exit, send error notification to Codex client, terminate
- **Codex client disconnect**: Detect via stdin EOF, send cancel to ACP agent, clean up, exit
