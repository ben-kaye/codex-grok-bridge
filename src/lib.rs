pub mod acp;
pub mod acp_proxy;
pub mod command_exec;
pub mod config;
pub mod error;
pub mod hybrid;
pub mod rollout;
pub mod sandbox;
pub mod translation;
pub mod transport;

use std::rc::Rc;

use agent_client_protocol::{Agent, ClientSideConnection};
use anyhow::{Context, Result};
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_app_server_protocol::{JSONRPCMessage, JSONRPCRequest, JSONRPCResponse};
use codex_protocol::protocol::{
    EventMsg, RolloutItem, SessionMeta, SessionMetaLine, SessionSource, ThreadRolledBackEvent,
};
use futures::StreamExt;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::acp::AcpEvent;
use crate::config::{GatewayConfig, ListenTransport};
use crate::error::GatewayError;
use crate::translation::acp_to_codex;
use crate::translation::approval;
use crate::translation::codex_to_acp;
use crate::translation::ext_method;
use crate::translation::id_map::{self, IdMap};
use crate::translation::state::{GatewayState, SessionLifecycle};
use crate::transport::TransportEvent;
use crate::transport::outgoing::OutgoingMessageSender;
use crate::transport::stdio;
use crate::transport::websocket;

const BRIDGE_NAME: &str = "codex-grok-bridge";
const BRIDGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Internal events produced by background tasks (e.g. `spawn_local` prompt)
/// and processed by the main event loop to update gateway state.
enum InternalEvent {
    /// The ACP `agent.prompt()` call completed (success or failure).
    TurnCompleted {
        thread_id: String,
        turn_id: String,
        prompt_id: String,
        outcome: PromptOutcome,
    },
}

enum PromptOutcome {
    Completed {
        stop_reason: String,
        usage: Option<agent_client_protocol::Usage>,
    },
    Failed(String),
}

fn spawn_prompt(
    agent: Rc<ClientSideConnection>,
    request: agent_client_protocol::PromptRequest,
    thread_id: String,
    turn_id: String,
    prompt_id: String,
    internal_tx: mpsc::Sender<InternalEvent>,
) {
    tokio::task::spawn_local(async move {
        let outcome = match agent.prompt(request).await {
            Ok(response) => PromptOutcome::Completed {
                stop_reason: format!("{:?}", response.stop_reason),
                usage: response.usage,
            },
            Err(error) => PromptOutcome::Failed(format!("{error:?}")),
        };
        let _ = internal_tx
            .send(InternalEvent::TurnCompleted {
                thread_id,
                turn_id,
                prompt_id,
                outcome,
            })
            .await;
    });
}

/// Run the gateway. This is the main entry point called from `main()`.
///
/// Dispatches to the appropriate transport mode based on `config.listen`:
/// - **Stdio**: single client, exit on EOF (original behavior)
/// - **WebSocket**: accept connections on a TCP port, each gets its own ACP subprocess
///
/// All work runs inside a `LocalSet` because ACP futures are `!Send`.
pub async fn run(config: GatewayConfig) -> Result<()> {
    let cwd = std::fs::canonicalize(&config.cwd).context("invalid cwd")?;
    info!(
        listen = %config.listen,
        agent_cmd = %config.agent_cmd,
        agent_args = ?config.agent_args,
        cwd = %cwd.display(),
        "starting codex-grok-bridge"
    );

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            match &config.listen {
                ListenTransport::Stdio => {
                    let (inbound_tx, inbound_rx) = mpsc::channel::<TransportEvent>(256);
                    let (outbound_tx, outbound_rx) = mpsc::channel::<String>(256);

                    stdio::spawn_reader(inbound_tx);
                    stdio::spawn_writer(outbound_rx);

                    run_session(&config, &cwd, inbound_rx, outbound_tx).await
                }
                ListenTransport::WebSocket { bind_address } => {
                    let listener = tokio::net::TcpListener::bind(bind_address)
                        .await
                        .with_context(|| {
                            format!("failed to bind WebSocket listener on {bind_address}")
                        })?;
                    info!(%bind_address, "listening for WebSocket connections");

                    loop {
                        let (tcp_stream, peer_addr) = listener.accept().await?;
                        debug!(%peer_addr, "TCP connection accepted");

                        let ws_stream = match tokio_tungstenite::accept_async(tcp_stream).await {
                            Ok(ws) => ws,
                            Err(e) => {
                                warn!(%peer_addr, "WebSocket handshake failed: {e}");
                                continue;
                            }
                        };

                        info!(%peer_addr, "WebSocket client connected");
                        let (ws_write, ws_read) = ws_stream.split();

                        let (inbound_tx, inbound_rx) = mpsc::channel::<TransportEvent>(256);
                        let (outbound_tx, outbound_rx) = mpsc::channel::<String>(256);
                        let (pong_tx, pong_rx) = mpsc::channel::<Vec<u8>>(16);

                        websocket::spawn_reader(inbound_tx, ws_read, pong_tx);
                        websocket::spawn_writer(outbound_rx, ws_write, pong_rx);

                        let cfg = config.clone();
                        let cwd = cwd.clone();
                        tokio::task::spawn_local(async move {
                            if let Err(e) = run_session(&cfg, &cwd, inbound_rx, outbound_tx).await {
                                warn!(%peer_addr, "session error: {e:#}");
                            }
                            info!(%peer_addr, "session ended");
                        });
                    }
                }
            }
        })
        .await
}

/// Run one gateway session for a single Codex client connection.
///
/// Spawns an ACP agent subprocess and runs the event loop that translates
/// between the Codex transport channels and the ACP connection.
///
/// This function is transport-agnostic — it only uses `mpsc` channels. Must be
/// called from within a `LocalSet` (ACP futures are `!Send`).
async fn run_session(
    config: &GatewayConfig,
    cwd: &std::path::Path,
    mut codex_inbound_rx: mpsc::Receiver<TransportEvent>,
    codex_outbound_tx: mpsc::Sender<String>,
) -> Result<()> {
    let outgoing = OutgoingMessageSender::new(codex_outbound_tx);

    let (acp_conn, mut acp_events) = acp::connect(&config.agent_cmd, &config.agent_args, cwd)
        .await
        .context("failed to connect to ACP agent")?;

    let mut state = GatewayState::new();
    let mut id_map = IdMap::load(state.rollout.gateway_home().join("routes.json"));
    let agent = Rc::new(acp_conn.agent);

    // Channel for background tasks (e.g. spawn_local prompt) to notify the
    // main loop about lifecycle transitions.
    let (internal_tx, mut internal_rx) = mpsc::channel::<InternalEvent>(64);

    info!("gateway event loop started");

    loop {
        tokio::select! {
            biased;

            // Drain ACP updates before handling prompt completion so the
            // authoritative completed items contain every streamed delta.
            event = acp_events.recv() => {
                match event {
                    Some(acp_event) => {
                        handle_acp_event(
                            acp_event,
                            &outgoing,
                            &mut state,
                            &mut id_map,
                        ).await;
                    }
                    None => {
                        info!("ACP event channel closed, agent likely exited");
                        break;
                    }
                }
            }

            event = codex_inbound_rx.recv() => {
                match event {
                    Some(TransportEvent::MessageReceived { message }) => {
                        handle_codex_message(
                            message,
                            &agent,
                            &outgoing,
                            &mut state,
                            &mut id_map,
                            cwd,
                            &internal_tx,
                        ).await;
                    }
                    Some(TransportEvent::Disconnected) | None => {
                        info!("codex client disconnected, shutting down");
                        break;
                    }
                }
            }

            event = internal_rx.recv() => {
                if let Some(internal_event) = event {
                    handle_internal_event(internal_event, &mut state, &outgoing).await;
                }
            }
        }
    }

    Ok(())
}

/// Handle an incoming JSON-RPC message from the Codex client.
async fn handle_codex_message(
    message: JSONRPCMessage,
    agent: &Rc<ClientSideConnection>,
    outgoing: &OutgoingMessageSender,
    state: &mut GatewayState,
    id_map: &mut IdMap,
    cwd: &std::path::Path,
    internal_tx: &mpsc::Sender<InternalEvent>,
) {
    match message {
        JSONRPCMessage::Request(req) => {
            handle_codex_request(req, agent, outgoing, state, id_map, cwd, internal_tx).await;
        }
        JSONRPCMessage::Response(resp) => {
            // This is a response to a server-initiated request (e.g., approval)
            outgoing.resolve_pending(resp).await;
        }
        JSONRPCMessage::Notification(notif) => {
            debug!(method = %notif.method, "codex notification (ignored for now)");
        }
        JSONRPCMessage::Error(err) => {
            warn!(id = ?err.id, error = %err.error.message, "codex client returned error");
            let resp = JSONRPCResponse {
                id: err.id,
                result: json!({ "error": err.error.message }),
            };
            outgoing.resolve_pending(resp).await;
        }
    }
}

/// Dispatch a Codex client request to the appropriate handler.
async fn handle_codex_request(
    req: JSONRPCRequest,
    agent: &Rc<ClientSideConnection>,
    outgoing: &OutgoingMessageSender,
    state: &mut GatewayState,
    id_map: &mut IdMap,
    cwd: &std::path::Path,
    internal_tx: &mpsc::Sender<InternalEvent>,
) {
    let id = req.id.clone();
    let params = req.params.clone().unwrap_or(json!({}));

    let result = match req.method.as_str() {
        "initialize" => handle_initialize(&params, agent.as_ref(), state).await,
        "initialized" => {
            // Notification-style acknowledgment from client, no response needed
            return;
        }
        "thread/start" => handle_thread_start(&params, agent.as_ref(), state, id_map, cwd).await,
        "thread/resume" => handle_thread_resume(&params, agent.as_ref(), state, id_map, cwd).await,
        "turn/start" => {
            handle_turn_start(
                &params,
                Rc::clone(agent),
                outgoing,
                state,
                id_map,
                internal_tx,
            )
            .await
        }
        "turn/steer" => {
            handle_turn_steer(&params, Rc::clone(agent), state, id_map, internal_tx).await
        }
        "turn/interrupt" => handle_turn_interrupt(&params, agent.as_ref(), state, id_map).await,
        "session/setMode" => handle_set_session_mode(&params, agent.as_ref(), state, id_map).await,
        "session/setConfigOption" => {
            handle_set_session_config_option(&params, agent.as_ref(), state, id_map).await
        }
        "command/exec" => handle_command_exec(&params, cwd).await,
        "fuzzyFileSearch" => handle_fuzzy_file_search(&params, cwd).await,
        "experimentalFeature/list" => Ok(json!({ "data": [], "nextCursor": null })),
        "thread/loaded/list" => {
            let loaded: Vec<Value> = state
                .threads
                .iter()
                .filter(|(_, lifecycle)| {
                    matches!(
                        lifecycle,
                        SessionLifecycle::Idle { .. } | SessionLifecycle::InTurn { .. }
                    )
                })
                .map(|(thread_id, _)| json!({ "threadId": thread_id }))
                .collect();
            Ok(json!({ "data": loaded, "nextCursor": null }))
        }
        "thread/list" => handle_thread_list(&params, agent.as_ref(), state, id_map).await,
        "thread/read" => handle_thread_read(&params, state).await,
        "thread/fork" => handle_thread_fork(&params, agent.as_ref(), state, id_map, cwd).await,
        "thread/rollback" => {
            handle_thread_rollback(&params, agent.as_ref(), state, id_map, cwd).await
        }
        "model/list" => handle_model_list(state),

        // Stub handlers — return empty/default responses
        "config/read" => Ok(json!({ "config": {}, "layers": [] })),
        "config/value/write" => Ok(json!({ "success": true })),
        "config/batchWrite" => Ok(json!({ "success": true })),
        "configRequirements/read" => Ok(json!({ "requirements": {} })),
        "mcpServerStatus/list" => Ok(json!({ "data": [], "nextCursor": null })),
        "config/mcpServer/reload" => Ok(json!({})),
        "skills/list" => Ok(json!({ "data": [], "nextCursor": null })),
        "thread/name/set" => handle_thread_name_set(&params, state, outgoing).await,
        "thread/archive" => handle_thread_archive(&params, state, outgoing).await,
        "thread/unarchive" => handle_thread_unarchive(&params, state, outgoing).await,
        "thread/compact/start" => Ok(json!({})),
        "thread/backgroundTerminals/clean" => Ok(json!({})),

        method => {
            debug!(method, "unhandled codex request method");
            Err(GatewayError::Translation(format!(
                "unsupported method: {method}"
            )))
        }
    };

    // After successful thread/start or thread/resume, emit a thread/started notification.
    // Extract the thread from the response value (works for both start and resume).
    let emit_thread_started = matches!(
        req.method.as_str(),
        "thread/start" | "thread/resume" | "thread/fork"
    ) && result.is_ok();

    match result {
        Ok(value) => {
            // Clone thread info before sending response (which consumes it).
            let thread_obj = if emit_thread_started {
                value.get("thread").cloned()
            } else {
                None
            };

            if let Err(e) = outgoing.send_response(id, value).await {
                error!("failed to send response: {e}");
            }
            if let Some(thread) = thread_obj {
                let _ = outgoing
                    .send_notification("thread/started", Some(json!({ "thread": thread })))
                    .await;
            }
        }
        Err(e) => {
            warn!(error = %e, "request failed");
            if let Err(send_err) = outgoing.send_error(id, e.to_jsonrpc_error()).await {
                error!("failed to send error response: {send_err}");
            }
        }
    }
}

/// Handle `initialize` - translate to ACP initialize, auto-authenticate if
/// needed, and return a rich response.
async fn handle_initialize(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &mut GatewayState,
) -> Result<Value, GatewayError> {
    let acp_req = codex_to_acp::translate_initialize(params);

    let acp_resp = agent
        .initialize(acp_req)
        .await
        .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;

    // Store agent capabilities and auth methods.
    state.agent_capabilities = Some(acp_resp.agent_capabilities.clone());
    state.auth_methods = acp_resp.auth_methods.clone();

    // Auto-authenticate if the agent advertises auth methods.
    if let Some(first_method) = acp_resp.auth_methods.first() {
        info!(method_id = %first_method.id.0, "auto-authenticating with agent");
        let auth_req = agent_client_protocol::AuthenticateRequest::new(first_method.id.clone());
        agent
            .authenticate(auth_req)
            .await
            .map_err(|e| GatewayError::Acp(format!("authenticate failed: {e:?}")))?;
    }

    state.initialized = true;
    if let Some(info) = params.get("clientInfo") {
        state.client_name = info.get("name").and_then(|v| v.as_str()).map(String::from);
        state.client_version = info
            .get("version")
            .and_then(|v| v.as_str())
            .map(String::from);
    }

    let protocol_version = format!("{}", acp_resp.protocol_version);
    info!(protocol_version = %protocol_version, "ACP initialize complete");

    let agent_version = acp_resp
        .agent_info
        .as_ref()
        .map(|i| i.version.as_str())
        .unwrap_or("unknown");

    Ok(json!({
        "userAgent": format!("{BRIDGE_NAME}/{BRIDGE_VERSION} (acp-agent/{agent_version})"),
        "protocolVersion": protocol_version,
        "capabilities": {
            "experimental": {},
        },
    }))
}

/// Handle `thread/start` - create a new ACP session.
async fn handle_thread_start(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &mut GatewayState,
    id_map: &mut IdMap,
    cwd: &std::path::Path,
) -> Result<Value, GatewayError> {
    if !state.initialized {
        return Err(GatewayError::NotInitialized);
    }

    let thread_id = id_map::new_thread_id();
    state.set_lifecycle(thread_id.clone(), SessionLifecycle::Creating);

    let acp_req = codex_to_acp::translate_thread_start(params, cwd);
    let acp_resp = agent
        .new_session(acp_req)
        .await
        .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;

    let session_id = acp_resp.session_id.to_string();
    id_map.create_thread_session_mapping(thread_id.clone(), session_id.clone());
    state.set_lifecycle(
        thread_id.clone(),
        SessionLifecycle::Idle {
            session_id: session_id.clone(),
        },
    );

    // Store model state from the agent if available.
    if let Some(models) = acp_resp.models {
        state.model_state = Some(models);
    }

    info!(%thread_id, %session_id, "thread/session created");

    // If the client requested a specific model and the agent supports model
    // selection, attempt to set it. This is best-effort — failure doesn't
    // block thread creation.
    if let Some(model) = params.get("model").and_then(|v| v.as_str()) {
        let has_models = state.model_state.is_some();
        if has_models {
            let model_req = codex_to_acp::translate_set_session_model(&session_id, model);
            match agent.set_session_model(model_req).await {
                Ok(_) => info!(%thread_id, %model, "session model set"),
                Err(e) => debug!(%thread_id, %model, "set_session_model failed (non-fatal): {e:?}"),
            }
        }
    }

    // Build SessionMetaLine for the rollout recorder.
    let now_str = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let now_secs = OffsetDateTime::now_utc().unix_timestamp();
    let session_meta = SessionMetaLine {
        meta: SessionMeta {
            id: codex_protocol::ThreadId::from_string(&thread_id).unwrap_or_default(),
            forked_from_id: None,
            timestamp: now_str.clone(),
            cwd: cwd.to_path_buf(),
            originator: BRIDGE_NAME.to_string(),
            cli_version: BRIDGE_VERSION.to_string(),
            source: SessionSource::VSCode,
            agent_nickname: None,
            agent_role: None,
            model_provider: Some("acp".to_string()),
            base_instructions: None,
            dynamic_tools: None,
        },
        git: None,
    };

    // Start recording this thread to a rollout file.
    state.rollout.start_thread(&session_meta);

    let selected_model = params
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("grok-4.6");
    Ok(json!({
        "thread": {
            "id": thread_id,
            "extra": null,
            "sessionId": thread_id,
            "forkedFromId": null,
            "parentThreadId": null,
            "cwd": cwd.to_string_lossy(),
            "cliVersion": BRIDGE_VERSION,
            "createdAt": now_secs,
            "updatedAt": now_secs,
            "recencyAt": now_secs,
            "modelProvider": "acp",
            "preview": "",
            "ephemeral": false,
            "section": null,
            "sectionEnteredAt": null,
            "historyMode": "full",
            "path": null,
            "source": "appServer",
            "status": { "type": "idle" },
            "canAcceptDirectInput": true,
            "threadSource": null,
            "agentNickname": null,
            "agentRole": null,
            "gitInfo": null,
            "name": null,
            "turns": [],
        },
        "model": selected_model,
        "modelProvider": "acp",
        "serviceTier": null,
        "cwd": cwd.to_string_lossy(),
        "runtimeWorkspaceRoots": [cwd.to_string_lossy()],
        "instructionSources": [],
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": { "type": "dangerFullAccess" },
        "activePermissionProfile": null,
        "reasoningEffort": null,
        "multiAgentMode": "explicitRequestOnly",
        "_meta": { "grokBridge": { "acpSessionId": session_id } },
    }))
}

/// Handle `turn/start` - send a prompt to the ACP session.
///
/// Takes an `Rc<ClientSideConnection>` (not a reference) so we can move a clone
/// into the `spawn_local` task that drives the prompt to completion.
async fn handle_turn_start(
    params: &Value,
    agent: Rc<ClientSideConnection>,
    outgoing: &OutgoingMessageSender,
    state: &mut GatewayState,
    id_map: &mut IdMap,
    internal_tx: &mpsc::Sender<InternalEvent>,
) -> Result<Value, GatewayError> {
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    let session_id = id_map
        .lookup_session(thread_id)
        .ok_or_else(|| GatewayError::SessionNotFound {
            thread_id: thread_id.into(),
        })?
        .clone();

    validate_turn_start_lifecycle(state, thread_id, &session_id)?;

    let turn_id = id_map::new_turn_id();
    let started_at_ms = unix_now_millis();

    state.set_lifecycle(
        thread_id.to_string(),
        SessionLifecycle::InTurn {
            session_id: session_id.clone(),
            turn_id: turn_id.clone(),
        },
    );
    state.turn_outputs.insert(
        turn_id.clone(),
        crate::translation::state::TurnOutput::new(started_at_ms),
    );

    // Send turn/started notification to Codex client
    let _ = outgoing
        .send_notification(
            "turn/started",
            Some(json!({
                "threadId": thread_id,
                "turn": {
                    "id": turn_id,
                    "items": [],
                    "itemsView": "full",
                    "status": "inProgress",
                    "error": null,
                    "startedAt": started_at_ms / 1_000,
                    "completedAt": null,
                    "durationMs": null,
                },
            })),
        )
        .await;

    // Notify the Codex client that this thread is now active.
    let _ = outgoing
        .send_notification(
            "thread/status/changed",
            Some(json!({
                "threadId": thread_id,
                "status": { "type": "active", "activeFlags": [] },
            })),
        )
        .await;

    // Create the reasoning item before Grok streams thought deltas. Codex
    // clients require item lifecycle notifications for delta targets.
    let _ = outgoing
        .send_notification(
            "item/started",
            Some(json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "item": {
                    "type": "reasoning",
                    "id": format!("{turn_id}-reasoning"),
                    "summary": [],
                    "content": [],
                },
            })),
        )
        .await;

    // Translate and send the prompt to ACP
    if let Some(effort) = params.get("effort").and_then(Value::as_str) {
        let mode_request = codex_to_acp::translate_set_session_mode(&session_id, effort);
        if let Err(error) = agent.set_session_mode(mode_request).await {
            warn!(%thread_id, %effort, "Grok reasoning effort selection failed: {error:?}");
        }
    }
    let acp_req = codex_to_acp::translate_turn_start(params, &session_id);
    let prompt_id = id_map::new_prompt_id();
    state
        .active_prompt_ids
        .insert(turn_id.clone(), prompt_id.clone());

    // The prompt task reports completion back to the event loop, which owns
    // the accumulated deltas and emits authoritative completion items.
    spawn_prompt(
        agent,
        acp_req,
        thread_id.to_string(),
        turn_id.clone(),
        prompt_id,
        internal_tx.clone(),
    );

    // Return the turn/start response immediately (streaming happens via notifications)
    Ok(json!({
        "turn": {
            "id": turn_id,
            "items": [],
            "itemsView": "full",
            "status": "inProgress",
            "error": null,
            "startedAt": started_at_ms / 1_000,
            "completedAt": null,
            "durationMs": null,
        },
    }))
}

fn validate_turn_start_lifecycle(
    state: &GatewayState,
    thread_id: &str,
    session_id: &str,
) -> Result<(), GatewayError> {
    match state.lifecycle(thread_id) {
        SessionLifecycle::Idle {
            session_id: active_session_id,
        } if active_session_id == session_id => Ok(()),
        SessionLifecycle::InTurn { turn_id, .. } => Err(GatewayError::Translation(format!(
            "cannot start a second turn while turn {turn_id} is active"
        ))),
        lifecycle => Err(GatewayError::Translation(format!(
            "cannot start a turn while thread lifecycle is {lifecycle:?}"
        ))),
    }
}

/// Adapt Codex steering to Grok Build's concurrent send-now prompt flow.
async fn handle_turn_steer(
    params: &Value,
    agent: Rc<ClientSideConnection>,
    state: &mut GatewayState,
    id_map: &IdMap,
    internal_tx: &mpsc::Sender<InternalEvent>,
) -> Result<Value, GatewayError> {
    let (thread_id, active_turn_id, prompt_id, request) =
        prepare_turn_steer(params, state, id_map)?;
    spawn_prompt(
        agent,
        request,
        thread_id,
        active_turn_id.clone(),
        prompt_id,
        internal_tx.clone(),
    );

    Ok(json!({ "turnId": active_turn_id }))
}

fn prepare_turn_steer(
    params: &Value,
    state: &mut GatewayState,
    id_map: &IdMap,
) -> Result<(String, String, String, agent_client_protocol::PromptRequest), GatewayError> {
    let thread_id = params
        .get("threadId")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;
    let expected_turn_id = params
        .get("expectedTurnId")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::Translation("missing expectedTurnId".into()))?;

    let (session_id, active_turn_id) = match state.lifecycle(thread_id) {
        SessionLifecycle::InTurn {
            session_id,
            turn_id,
        } => (session_id.clone(), turn_id.clone()),
        _ => {
            return Err(GatewayError::Translation(
                "cannot steer a thread without an active turn".into(),
            ));
        }
    };
    if active_turn_id != expected_turn_id {
        return Err(GatewayError::Translation(format!(
            "expected active turn {expected_turn_id}, found {active_turn_id}"
        )));
    }
    let mapped_session_id =
        id_map
            .lookup_session(thread_id)
            .ok_or_else(|| GatewayError::SessionNotFound {
                thread_id: thread_id.into(),
            })?;
    if mapped_session_id != &session_id {
        return Err(GatewayError::Translation(
            "active turn session does not match the thread mapping".into(),
        ));
    }

    let request = codex_to_acp::translate_turn_steer(params, &session_id);
    let prompt_id = id_map::new_prompt_id();
    state
        .active_prompt_ids
        .insert(active_turn_id.clone(), prompt_id.clone());

    Ok((thread_id.to_string(), active_turn_id, prompt_id, request))
}

/// Handle `turn/interrupt` - cancel the active ACP prompt.
async fn handle_turn_interrupt(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &GatewayState,
    id_map: &IdMap,
) -> Result<Value, GatewayError> {
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;
    let requested_turn_id = params
        .get("turnId")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::Translation("missing turnId".into()))?;

    let mapped_session_id = id_map
        .lookup_session(thread_id)
        .ok_or_else(|| GatewayError::SessionNotFound {
            thread_id: thread_id.into(),
        })?
        .clone();

    let session_id = match state.lifecycle(thread_id) {
        SessionLifecycle::InTurn {
            session_id,
            turn_id,
        } if turn_id == requested_turn_id => session_id.clone(),
        SessionLifecycle::InTurn { turn_id, .. } => {
            return Err(GatewayError::Translation(format!(
                "cannot interrupt turn {requested_turn_id}; active turn is {turn_id}"
            )));
        }
        _ => {
            return Err(GatewayError::Translation(
                "cannot interrupt a thread without an active turn".into(),
            ));
        }
    };
    if session_id != mapped_session_id {
        return Err(GatewayError::Translation(
            "active turn session does not match the thread mapping".into(),
        ));
    }

    agent
        .cancel(agent_client_protocol::CancelNotification::new(session_id))
        .await
        .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;

    Ok(json!({}))
}

/// Handle `thread/resume` — load an existing ACP session.
async fn handle_thread_resume(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &mut GatewayState,
    id_map: &mut IdMap,
    cwd: &std::path::Path,
) -> Result<Value, GatewayError> {
    if !state.initialized {
        return Err(GatewayError::NotInitialized);
    }

    // The Codex client sends threadId for the thread to resume.
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    // Look up the existing session for this thread, or use threadId as session_id
    // if this is a session the agent knows about from a previous run.
    let session_id = id_map
        .lookup_session(thread_id)
        .cloned()
        .unwrap_or_else(|| thread_id.to_string());

    state.set_lifecycle(thread_id.to_string(), SessionLifecycle::Loading);

    // Check if the agent supports the unstable session/resume method (skips
    // replaying message history). Fall back to session/load if not supported.
    let supports_resume = state
        .agent_capabilities
        .as_ref()
        .and_then(|c| c.session_capabilities.resume.as_ref())
        .is_some();

    let (modes, config) = if supports_resume {
        let acp_req = codex_to_acp::translate_thread_resume_session(params, &session_id, cwd);
        let acp_resp = agent
            .resume_session(acp_req)
            .await
            .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;
        info!(%thread_id, "used ACP session/resume (no history replay)");
        (acp_resp.modes, acp_resp.config_options)
    } else {
        let acp_req = codex_to_acp::translate_thread_resume(params, &session_id, cwd);
        let acp_resp = agent
            .load_session(acp_req)
            .await
            .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;
        (acp_resp.modes, acp_resp.config_options)
    };

    // Store modes and config from the response.
    if let Some(modes) = modes {
        state.session_modes.insert(session_id.clone(), modes);
    }
    if let Some(config) = config {
        state.session_config.insert(session_id.clone(), config);
    }

    // Ensure the mapping exists.
    if id_map.lookup_session(thread_id).is_none() {
        id_map.create_thread_session_mapping(thread_id.to_string(), session_id.clone());
    }

    state.set_lifecycle(
        thread_id.to_string(),
        SessionLifecycle::Idle {
            session_id: session_id.clone(),
        },
    );

    // If the client requested a specific model, attempt to set it.
    if let Some(model) = params.get("model").and_then(|v| v.as_str()) {
        let model_req = codex_to_acp::translate_set_session_model(&session_id, model);
        match agent.set_session_model(model_req).await {
            Ok(_) => info!(%thread_id, %model, "session model set on resume"),
            Err(e) => {
                debug!(%thread_id, %model, "set_session_model on resume failed (non-fatal): {e:?}")
            }
        }
    }

    info!(%thread_id, %session_id, "thread/session resumed");

    let now_secs = unix_now_secs();

    // Try to read session meta from the rollout file for richer response.
    let (meta_provider, meta_created, meta_source) =
        if let Some(path) = state.rollout.find_thread_file(thread_id) {
            if let Ok(meta) = crate::rollout::list::read_session_meta_line(&path).await {
                let created = parse_rfc3339_to_unix(&meta.meta.timestamp).unwrap_or(now_secs);
                let provider = meta.meta.model_provider.unwrap_or_else(|| "acp".into());
                (provider, created, meta.meta.source)
            } else {
                ("acp".into(), now_secs, SessionSource::VSCode)
            }
        } else {
            ("acp".into(), now_secs, SessionSource::VSCode)
        };

    Ok(json!({
        "thread": {
            "id": thread_id,
            "cwd": cwd.to_string_lossy(),
            "cliVersion": BRIDGE_VERSION,
            "createdAt": meta_created,
            "updatedAt": now_secs,
            "modelProvider": meta_provider,
            "preview": "",
            "source": meta_source,
            "status": { "type": "idle" },
            "turns": [],
        },
        "model": params.get("model").and_then(Value::as_str).unwrap_or("grok-4.6"),
        "modelProvider": meta_provider,
        "serviceTier": null,
        "cwd": cwd.to_string_lossy(),
        "runtimeWorkspaceRoots": [cwd.to_string_lossy()],
        "instructionSources": [],
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": { "type": "dangerFullAccess" },
        "activePermissionProfile": null,
        "reasoningEffort": null,
        "multiAgentMode": "explicitRequestOnly",
        "initialTurnsPage": null,
        "turnsBackwardsCursor": null,
        "itemsBackwardsCursor": null,
        "_meta": { "grokBridge": { "acpSessionId": session_id } },
    }))
}

/// Handle `session/setMode` — change the agent's session mode.
async fn handle_set_session_mode(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    _state: &mut GatewayState,
    id_map: &mut IdMap,
) -> Result<Value, GatewayError> {
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;
    let mode_id = params
        .get("modeId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing modeId".into()))?;

    let session_id = id_map
        .lookup_session(thread_id)
        .ok_or_else(|| GatewayError::SessionNotFound {
            thread_id: thread_id.into(),
        })?
        .clone();

    let acp_req = codex_to_acp::translate_set_session_mode(&session_id, mode_id);
    agent
        .set_session_mode(acp_req)
        .await
        .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;

    info!(%thread_id, %mode_id, "session mode changed");

    // The agent will send a CurrentModeUpdate notification confirming the change.
    Ok(json!({}))
}

/// Handle `session/setConfigOption` — change an agent config option.
async fn handle_set_session_config_option(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &mut GatewayState,
    id_map: &mut IdMap,
) -> Result<Value, GatewayError> {
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;
    let config_id = params
        .get("configId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing configId".into()))?;
    let value = params
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing value".into()))?;

    let session_id = id_map
        .lookup_session(thread_id)
        .ok_or_else(|| GatewayError::SessionNotFound {
            thread_id: thread_id.into(),
        })?
        .clone();

    let acp_req = codex_to_acp::translate_set_session_config_option(&session_id, config_id, value);
    let acp_resp = agent
        .set_session_config_option(acp_req)
        .await
        .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;

    // Store updated config in state.
    state
        .session_config
        .insert(session_id.clone(), acp_resp.config_options.clone());

    info!(%thread_id, %config_id, %value, "session config option changed");

    // Return the full updated config options so the client can refresh its UI.
    let options: Vec<Value> = acp_resp
        .config_options
        .iter()
        .map(|opt| {
            json!({
                "id": opt.id.0.as_ref(),
                "name": opt.name,
            })
        })
        .collect();

    Ok(json!({ "configOptions": options }))
}

/// Handle `thread/list` — return persisted threads from rollout files.
///
/// Uses the vendored `rollout::list::get_threads()` for paginated listing
/// with proper Codex-compatible Thread JSON. When the agent supports the
/// unstable `session/list` method, agent-known sessions that don't have
/// local rollout files are appended to the listing.
async fn handle_thread_list(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &GatewayState,
    id_map: &IdMap,
) -> Result<Value, GatewayError> {
    use crate::rollout::INTERACTIVE_SESSION_SOURCES;
    use crate::rollout::list::{self, ThreadSortKey, parse_cursor};

    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;

    let cursor = params
        .get("cursor")
        .and_then(|v| v.as_str())
        .and_then(parse_cursor);

    let sort_key = match params.get("sortKey").and_then(|v| v.as_str()) {
        Some("updatedAt") => ThreadSortKey::UpdatedAt,
        _ => ThreadSortKey::CreatedAt,
    };

    let gateway_home = state.rollout.gateway_home();
    let page = list::get_threads(
        gateway_home,
        limit,
        cursor.as_ref(),
        sort_key,
        INTERACTIVE_SESSION_SOURCES,
        None,
        "acp",
    )
    .await
    .map_err(|e| GatewayError::Translation(format!("thread/list failed: {e}")))?;

    // Load thread names for all items.
    let thread_ids: std::collections::HashSet<codex_protocol::ThreadId> = page
        .items
        .iter()
        .filter_map(|item| item.thread_id)
        .collect();
    let names = crate::rollout::session_index::find_thread_names_by_ids(gateway_home, &thread_ids)
        .await
        .unwrap_or_default();

    let mut data: Vec<Value> = page
        .items
        .iter()
        .map(|item| {
            let name = item.thread_id.and_then(|id| names.get(&id)).cloned();
            build_thread_json_from_list_item(item, name, &state.threads)
        })
        .collect();

    // If the agent supports session/list, query it to find sessions we may
    // not have local rollout files for (e.g. from a previous gateway run).
    let supports_list = state
        .agent_capabilities
        .as_ref()
        .and_then(|c| c.session_capabilities.list.as_ref())
        .is_some();

    if supports_list && cursor.is_none() {
        // Only query agent on the first page (no cursor) to avoid complexity.
        let mut list_req = agent_client_protocol::ListSessionsRequest::new();
        if let Some(cwd_str) = params.get("cwd").and_then(|v| v.as_str()) {
            list_req = list_req.cwd(std::path::PathBuf::from(cwd_str));
        }
        if let Ok(list_resp) = agent.list_sessions(list_req).await {
            // Collect IDs we already have locally (owned to avoid borrow conflict).
            let known_ids: std::collections::HashSet<String> = data
                .iter()
                .filter_map(|v| v.get("id").and_then(|id| id.as_str()).map(String::from))
                .collect();

            for session_info in &list_resp.sessions {
                let sid = session_info.session_id.to_string();
                if let Some(thread_id) = id_map.lookup_thread(&sid)
                    && known_ids.contains(thread_id)
                {
                    continue; // Durable mapping already has a local Codex thread.
                }
                if known_ids.contains(&sid) {
                    continue; // Already have this thread locally
                }
                let updated = session_info
                    .updated_at
                    .as_deref()
                    .and_then(parse_rfc3339_to_unix)
                    .unwrap_or(0);
                data.push(json!({
                    "id": sid,
                    "preview": "",
                    "modelProvider": "acp",
                    "createdAt": updated,
                    "updatedAt": updated,
                    "status": { "type": "notLoaded" },
                    "cwd": session_info.cwd.to_string_lossy(),
                    "cliVersion": "",
                    "source": "appServer",
                    "name": session_info.title,
                    "turns": [],
                }));
            }
        }
    }

    let next_cursor = page.next_cursor.and_then(|c| serde_json::to_value(c).ok());

    Ok(json!({ "data": data, "nextCursor": next_cursor }))
}

/// Handle `thread/read` — return a thread with its turns from rollout file.
///
/// Uses `load_rollout_items()` + `build_turns_from_rollout_items()` for
/// Codex-compatible Thread JSON with turn reconstruction.
async fn handle_thread_read(params: &Value, state: &GatewayState) -> Result<Value, GatewayError> {
    use crate::rollout::list;
    use crate::rollout::recorder::RolloutRecorder;

    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    let include_turns = params
        .get("includeTurns")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let gateway_home = state.rollout.gateway_home();

    // Find the rollout file (in-memory map first, then filesystem scan).
    let file_path = state
        .rollout
        .find_thread_file(thread_id)
        .ok_or_else(|| GatewayError::Translation(format!("thread not found: {thread_id}")))?;

    // Read the session meta from the first line.
    let meta = list::read_session_meta_line(&file_path)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to read session meta: {e}")))?;

    // Load rollout items for turn reconstruction.
    let items = RolloutRecorder::load_rollout_items(&file_path);

    let turns = if include_turns {
        let turns = build_turns_from_rollout_items(&items);
        serde_json::to_value(turns).unwrap_or(json!([]))
    } else {
        json!([])
    };

    // Get thread name from session index.
    let name = if let Ok(tid) = codex_protocol::ThreadId::from_string(thread_id) {
        crate::rollout::session_index::find_thread_name_by_id(gateway_home, &tid)
            .await
            .ok()
            .flatten()
    } else {
        None
    };

    let now_secs = unix_now_secs();
    let created_secs = parse_rfc3339_to_unix(&meta.meta.timestamp).unwrap_or(now_secs);
    let status = lifecycle_to_thread_status(state.lifecycle(thread_id));

    Ok(json!({
        "thread": {
            "id": thread_id,
            "preview": "",
            "modelProvider": meta.meta.model_provider.as_deref().unwrap_or("acp"),
            "createdAt": created_secs,
            "updatedAt": now_secs,
            "status": status,
            "path": file_path.to_string_lossy(),
            "cwd": meta.meta.cwd.to_string_lossy(),
            "cliVersion": meta.meta.cli_version,
            "source": meta.meta.source,
            "gitInfo": meta.git,
            "name": name,
            "turns": turns,
        },
    }))
}

/// Handle `thread/fork` — fork an existing thread into a new one.
///
/// Loads all rollout items from the source thread, creates a new thread with a
/// new ID, copies the items into the new rollout file, and creates a fresh ACP
/// session for the forked thread.
async fn handle_thread_fork(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &mut GatewayState,
    id_map: &mut IdMap,
    _cwd: &std::path::Path,
) -> Result<Value, GatewayError> {
    use crate::rollout::list;
    use crate::rollout::recorder::RolloutRecorder;

    if !state.initialized {
        return Err(GatewayError::NotInitialized);
    }

    let source_thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    let gateway_home = state.rollout.gateway_home().to_path_buf();

    // Find and load the source rollout file.
    let source_path = list::find_thread_path_by_id_str(&gateway_home, source_thread_id)
        .await
        .map_err(|e| GatewayError::Translation(format!("fork: scan failed: {e}")))?
        .ok_or_else(|| {
            GatewayError::Translation(format!("fork: thread not found: {source_thread_id}"))
        })?;

    let items = RolloutRecorder::load_rollout_items(&source_path);

    // Read source session meta for inheriting properties.
    let source_meta = list::read_session_meta_line(&source_path)
        .await
        .map_err(|e| GatewayError::Translation(format!("fork: failed to read meta: {e}")))?;

    // Generate a new thread ID for the fork.
    let new_thread_id = id_map::new_thread_id();
    state.set_lifecycle(new_thread_id.clone(), SessionLifecycle::Creating);

    let now_str = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());

    // Use fork params to override cwd if provided, otherwise inherit from source.
    let fork_cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| source_meta.meta.cwd.clone());

    let source_tid = codex_protocol::ThreadId::from_string(source_thread_id).ok();

    let model_provider = params
        .get("modelProvider")
        .and_then(|v| v.as_str())
        .unwrap_or(source_meta.meta.model_provider.as_deref().unwrap_or("acp"))
        .to_string();

    let new_meta = SessionMetaLine {
        meta: SessionMeta {
            id: codex_protocol::ThreadId::from_string(&new_thread_id).unwrap_or_default(),
            forked_from_id: source_tid,
            timestamp: now_str.clone(),
            cwd: fork_cwd.clone(),
            originator: BRIDGE_NAME.to_string(),
            cli_version: BRIDGE_VERSION.to_string(),
            source: SessionSource::VSCode,
            agent_nickname: None,
            agent_role: None,
            model_provider: Some(model_provider.clone()),
            base_instructions: params.get("baseInstructions").and_then(|v| v.as_str()).map(
                |text| codex_protocol::models::BaseInstructions {
                    text: text.to_string(),
                },
            ),
            dynamic_tools: None,
        },
        git: source_meta.git,
    };

    // Start recording the new thread.
    state.rollout.start_thread(&new_meta);

    // Copy all non-SessionMeta items from the source rollout into the new file.
    for item in &items {
        if matches!(item, RolloutItem::SessionMeta(_)) {
            continue;
        }
        state.rollout.record_item(&new_thread_id, item.clone());
    }

    // Check if the agent supports the unstable session/fork method.
    // If so, delegate to the agent (it preserves internal conversation context).
    // Otherwise fall back to creating a new session.
    let supports_fork = state
        .agent_capabilities
        .as_ref()
        .and_then(|c| c.session_capabilities.fork.as_ref())
        .is_some();

    let source_session_id = id_map.lookup_session(source_thread_id).cloned();

    let session_id = if let (true, Some(src_sid)) = (supports_fork, source_session_id) {
        let acp_req = codex_to_acp::translate_thread_fork_session(params, &src_sid, &fork_cwd);
        let acp_resp = agent
            .fork_session(acp_req)
            .await
            .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;
        let sid = acp_resp.session_id.to_string();
        if let Some(modes) = acp_resp.modes {
            state.session_modes.insert(sid.clone(), modes);
        }
        if let Some(config) = acp_resp.config_options {
            state.session_config.insert(sid.clone(), config);
        }
        info!(%new_thread_id, session_id = %sid, source = %source_thread_id, "thread forked via ACP session/fork");
        sid
    } else {
        let acp_params = json!({ "cwd": fork_cwd.to_string_lossy() });
        let acp_req = codex_to_acp::translate_thread_start(&acp_params, &fork_cwd);
        let acp_resp = agent
            .new_session(acp_req)
            .await
            .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;
        let sid = acp_resp.session_id.to_string();
        info!(%new_thread_id, session_id = %sid, source = %source_thread_id, "thread forked (new session fallback)");
        sid
    };

    id_map.create_thread_session_mapping(new_thread_id.clone(), session_id.clone());
    state.set_lifecycle(
        new_thread_id.clone(),
        SessionLifecycle::Idle {
            session_id: session_id.clone(),
        },
    );

    // Build turns from all items (including copied ones) for the response.
    let turns = build_turns_from_rollout_items(&items);
    let turns_json = serde_json::to_value(&turns).unwrap_or(json!([]));

    let created_secs = parse_rfc3339_to_unix(&now_str).unwrap_or_else(unix_now_secs);

    Ok(json!({
        "thread": {
            "id": new_thread_id,
            "cwd": fork_cwd.to_string_lossy(),
            "cliVersion": BRIDGE_VERSION,
            "createdAt": created_secs,
            "updatedAt": created_secs,
            "modelProvider": model_provider,
            "preview": "",
            "source": "appServer",
            "status": { "type": "idle" },
            "turns": turns_json,
        },
        "model": "acp-agent",
        "modelProvider": model_provider,
        "cwd": fork_cwd.to_string_lossy(),
        "approvalPolicy": "on-request",
        "sandbox": { "type": "dangerFullAccess" },
    }))
}

/// Handle `thread/rollback` — roll back the last N turns from a thread.
///
/// Records a `ThreadRolledBack` event to the rollout file, rebuilds turns
/// from the updated rollout items, and creates a fresh ACP session since the
/// old session context is stale after rollback.
async fn handle_thread_rollback(
    params: &Value,
    agent: &(impl Agent + ?Sized),
    state: &mut GatewayState,
    id_map: &mut IdMap,
    _cwd: &std::path::Path,
) -> Result<Value, GatewayError> {
    use crate::rollout::list;
    use crate::rollout::recorder::RolloutRecorder;

    if !state.initialized {
        return Err(GatewayError::NotInitialized);
    }

    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    let num_turns = params
        .get("numTurns")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .ok_or_else(|| GatewayError::Translation("missing numTurns".into()))?;

    if num_turns < 1 {
        return Err(GatewayError::Translation("numTurns must be >= 1".into()));
    }

    // Find the rollout file.
    let file_path = state
        .rollout
        .find_thread_file(thread_id)
        .ok_or_else(|| GatewayError::Translation(format!("thread not found: {thread_id}")))?;

    // Record the ThreadRolledBack event to the rollout file.
    state.rollout.record_item(
        thread_id,
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
            num_turns,
        })),
    );

    // Reload all items (now including the rollback marker) and rebuild turns.
    let items = RolloutRecorder::load_rollout_items(&file_path);
    let turns = build_turns_from_rollout_items(&items);
    let turns_json = serde_json::to_value(&turns).unwrap_or(json!([]));

    // Read session meta for response fields.
    let meta = list::read_session_meta_line(&file_path)
        .await
        .map_err(|e| GatewayError::Translation(format!("rollback: failed to read meta: {e}")))?;

    let rollback_cwd = meta.meta.cwd.clone();

    // Create a new ACP session (old session context is stale after rollback).
    let acp_req = codex_to_acp::translate_thread_start(
        &json!({ "cwd": rollback_cwd.to_string_lossy() }),
        &rollback_cwd,
    );
    let acp_resp = agent
        .new_session(acp_req)
        .await
        .map_err(|e| GatewayError::Acp(format!("{e:?}")))?;

    let new_session_id = acp_resp.session_id.to_string();
    id_map.create_thread_session_mapping(thread_id.to_string(), new_session_id.clone());
    state.set_lifecycle(
        thread_id.to_string(),
        SessionLifecycle::Idle {
            session_id: new_session_id.clone(),
        },
    );

    info!(%thread_id, %new_session_id, %num_turns, "thread rolled back");

    let now_secs = unix_now_secs();
    let created_secs = parse_rfc3339_to_unix(&meta.meta.timestamp).unwrap_or(now_secs);

    Ok(json!({
        "thread": {
            "id": thread_id,
            "preview": "",
            "modelProvider": meta.meta.model_provider.as_deref().unwrap_or("acp"),
            "createdAt": created_secs,
            "updatedAt": now_secs,
            "status": { "type": "idle" },
            "cwd": rollback_cwd.to_string_lossy(),
            "cliVersion": meta.meta.cli_version,
            "source": meta.meta.source,
            "turns": turns_json,
        },
    }))
}

/// Handle `command/exec` — execute a command locally and return its output.
///
/// This is a Codex client request for one-off command execution (e.g. running
/// a build tool). The gateway handles it directly without involving the ACP
/// agent.
async fn handle_command_exec(params: &Value, cwd: &std::path::Path) -> Result<Value, GatewayError> {
    command_exec::run(params, cwd)
        .await
        .map_err(|e| GatewayError::Translation(format!("{e}")))
}

/// Handle `fuzzyFileSearch` — perform a one-shot fuzzy file search.
///
/// Uses `codex_file_search::run` inside `spawn_blocking` since the search is
/// CPU-bound. Returns results matching the `FuzzyFileSearchResponse` schema.
async fn handle_fuzzy_file_search(
    params: &Value,
    cwd: &std::path::Path,
) -> Result<Value, GatewayError> {
    let query = params
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("fuzzyFileSearch: missing 'query'".into()))?
        .to_string();

    let roots: Vec<std::path::PathBuf> = params
        .get("roots")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(std::path::PathBuf::from))
                .collect()
        })
        .unwrap_or_else(|| vec![cwd.to_path_buf()]);

    if roots.is_empty() {
        return Ok(json!({ "files": [] }));
    }

    const MATCH_LIMIT: usize = 50;
    const MAX_THREADS: usize = 12;

    let cores = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    let threads = cores.clamp(1, MAX_THREADS);
    #[allow(clippy::expect_used)]
    let limit = std::num::NonZero::new(MATCH_LIMIT).expect("MATCH_LIMIT is non-zero");
    #[allow(clippy::expect_used)]
    let threads = std::num::NonZero::new(threads).expect("threads is non-zero");

    let files = match tokio::task::spawn_blocking(move || {
        codex_file_search::run(
            &query,
            roots,
            codex_file_search::FileSearchOptions {
                limit,
                threads,
                compute_indices: true,
                ..Default::default()
            },
            None,
        )
    })
    .await
    {
        Ok(Ok(res)) => {
            let mut files: Vec<Value> = res
                .matches
                .into_iter()
                .map(|m| {
                    let file_name = m
                        .path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    json!({
                        "root": m.root.to_string_lossy(),
                        "path": m.path.to_string_lossy(),
                        "file_name": file_name,
                        "score": m.score,
                        "indices": m.indices,
                    })
                })
                .collect();
            files.sort_by(|a, b| {
                let sa = a["score"].as_u64().unwrap_or(0);
                let sb = b["score"].as_u64().unwrap_or(0);
                sb.cmp(&sa).then_with(|| {
                    let pa = a["path"].as_str().unwrap_or("");
                    let pb = b["path"].as_str().unwrap_or("");
                    pa.cmp(pb)
                })
            });
            files
        }
        Ok(Err(e)) => {
            warn!("fuzzyFileSearch failed: {e}");
            Vec::new()
        }
        Err(e) => {
            warn!("fuzzyFileSearch join failed: {e}");
            Vec::new()
        }
    };

    Ok(json!({ "files": files }))
}

/// Handle an ACP event (notification or permission request from the agent).
async fn handle_acp_event(
    event: AcpEvent,
    outgoing: &OutgoingMessageSender,
    state: &mut GatewayState,
    id_map: &mut IdMap,
) {
    match event {
        AcpEvent::SessionNotification(notif) => {
            let session_id = notif.session_id.to_string();
            let thread_id = id_map
                .lookup_thread(&session_id)
                .cloned()
                .unwrap_or_else(|| format!("unknown-{session_id}"));

            // Determine turn_id from state
            let turn_id = match state.lifecycle(&thread_id) {
                SessionLifecycle::InTurn { turn_id, .. } => turn_id.clone(),
                _ => "unknown-turn".to_string(),
            };

            // If the agent sent a SessionInfoUpdate with a title, persist it to
            // the session index so thread/list picks it up.
            if let agent_client_protocol::SessionUpdate::SessionInfoUpdate(ref info) = notif.update
                && let agent_client_protocol::MaybeUndefined::Value(ref title) = info.title
                && let Ok(tid) = codex_protocol::ThreadId::from_string(&thread_id)
            {
                let home = state.rollout.gateway_home().to_path_buf();
                let title = title.clone();
                tokio::task::spawn_local(async move {
                    if let Err(e) =
                        crate::rollout::session_index::append_thread_name(&home, tid, &title).await
                    {
                        warn!("failed to persist agent-provided title: {e}");
                    }
                });
            }

            let result = if let Some(output) = state.turn_outputs.get_mut(&turn_id) {
                acp_to_codex::translate_ordered_session_update(
                    &notif.update,
                    &thread_id,
                    &turn_id,
                    id_map,
                    output,
                )
            } else {
                acp_to_codex::translate_session_update(
                    &notif.update,
                    &thread_id,
                    &turn_id,
                    &format!("{turn_id}-msg-0"),
                    id_map,
                )
            };

            for (method, params) in result.notifications {
                if let Err(e) = outgoing.send_notification(&method, Some(params)).await {
                    warn!(method, "failed to send notification: {e}");
                }
            }

            // ACP content collections replace previous values. Retain the
            // first-seen tool order while replacing repeated updates for the
            // same call (Grok repeats edit content on completion).
            if let Some(diff_update) = result.diff_update {
                let accumulated = state.turn_diffs.entry(turn_id.clone()).or_default();
                if let Some(existing) = accumulated
                    .iter_mut()
                    .find(|entry| entry.tool_call_id == diff_update.tool_call_id)
                {
                    existing.diffs = diff_update.diffs;
                } else {
                    accumulated.push(diff_update);
                }

                let aggregated = build_aggregated_diff(accumulated);

                if let Err(e) = outgoing
                    .send_notification(
                        "turn/diff/updated",
                        Some(json!({
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "diff": aggregated,
                        })),
                    )
                    .await
                {
                    warn!("failed to send turn/diff/updated: {e}");
                }
            }
        }
        AcpEvent::PermissionRequest {
            request,
            response_tx,
        } => {
            let session_id = request.session_id.to_string();
            let thread_id = id_map
                .lookup_thread(&session_id)
                .cloned()
                .unwrap_or_else(|| format!("unknown-{session_id}"));

            let turn_id = match state.lifecycle(&thread_id) {
                SessionLifecycle::InTurn { turn_id, .. } => turn_id.clone(),
                _ => "unknown-turn".to_string(),
            };

            // Get or create item ID for this tool call
            let tool_call_id = request.tool_call.tool_call_id.0.as_ref();
            let item_id = id_map
                .lookup_item(tool_call_id)
                .cloned()
                .unwrap_or_else(|| id_map.create_item_for_tool(tool_call_id));

            let (method, params) =
                approval::translate_permission_to_codex(&request, &thread_id, &turn_id, &item_id);

            // Send request to Codex client and await response
            match outgoing.send_request(method, Some(params)).await {
                Ok(rx) => {
                    let options = request.options.clone();
                    tokio::task::spawn_local(async move {
                        match rx.await {
                            Ok(resp) => {
                                let acp_resp = approval::translate_codex_approval_to_acp(
                                    &resp.result,
                                    &options,
                                );
                                let _ = response_tx.send(acp_resp);
                            }
                            Err(_) => {
                                let _ =
                                    response_tx
                                        .send(agent_client_protocol::RequestPermissionResponse::new(
                                        agent_client_protocol::RequestPermissionOutcome::Cancelled,
                                    ));
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("failed to send approval request to codex client: {e}");
                    let _ =
                        response_tx.send(agent_client_protocol::RequestPermissionResponse::new(
                            agent_client_protocol::RequestPermissionOutcome::Cancelled,
                        ));
                }
            }
        }
        AcpEvent::ExtMethodRequest {
            request,
            response_tx,
        } => {
            handle_ext_method_request(request, response_tx, outgoing, state).await;
        }
    }
}

/// Handle an ACP `ext_method` request by routing to the appropriate Codex
/// server request (`item/tool/call` or `item/tool/requestUserInput`).
///
/// Unknown extension methods receive a `method_not_found` error response.
async fn handle_ext_method_request(
    request: agent_client_protocol::ExtRequest,
    response_tx: tokio::sync::oneshot::Sender<
        agent_client_protocol::Result<agent_client_protocol::ExtResponse>,
    >,
    outgoing: &OutgoingMessageSender,
    state: &GatewayState,
) {
    let method = request.method.as_ref();

    // Find the active thread/turn from state (ext_method carries no session_id).
    let (thread_id, turn_id) = find_active_turn(state);

    // Parse the raw params to a serde_json::Value for inspection.
    let params: Value = serde_json::from_str(request.params.get()).unwrap_or(json!({}));

    match method {
        ext_method::EXT_TOOL_CALL => {
            let call_id = id_map::new_item_id();
            let (codex_method, codex_params) =
                ext_method::translate_ext_tool_call(&params, &thread_id, &turn_id, &call_id);

            forward_ext_to_codex(
                codex_method,
                codex_params,
                response_tx,
                outgoing,
                ext_method::translate_tool_call_response,
            )
            .await;
        }
        ext_method::EXT_REQUEST_USER_INPUT => {
            let item_id = id_map::new_item_id();
            let (codex_method, codex_params) = ext_method::translate_ext_request_user_input(
                &params, &thread_id, &turn_id, &item_id,
            );

            forward_ext_to_codex(
                codex_method,
                codex_params,
                response_tx,
                outgoing,
                ext_method::translate_user_input_response,
            )
            .await;
        }
        _ => {
            warn!(method, "unknown ext method from agent");
            let _ = response_tx.send(Err(agent_client_protocol::Error::method_not_found()
                .data(format!("unknown ext method: {method}"))));
        }
    }
}

/// Send a Codex server request on behalf of an ext_method call, await the
/// response, translate it back, and send it through the oneshot channel.
async fn forward_ext_to_codex(
    codex_method: String,
    codex_params: Value,
    response_tx: tokio::sync::oneshot::Sender<
        agent_client_protocol::Result<agent_client_protocol::ExtResponse>,
    >,
    outgoing: &OutgoingMessageSender,
    translate_response: fn(&Value) -> std::sync::Arc<serde_json::value::RawValue>,
) {
    match outgoing
        .send_request(codex_method, Some(codex_params))
        .await
    {
        Ok(rx) => {
            tokio::task::spawn_local(async move {
                match rx.await {
                    Ok(resp) => {
                        let raw = translate_response(&resp.result);
                        let _ = response_tx.send(Ok(agent_client_protocol::ExtResponse::new(raw)));
                    }
                    Err(_) => {
                        let _ = response_tx
                            .send(Err(agent_client_protocol::Error::internal_error()
                                .data("codex client disconnected during ext method")));
                    }
                }
            });
        }
        Err(e) => {
            error!("failed to send ext method request to codex client: {e}");
            let _ = response_tx.send(Err(agent_client_protocol::Error::internal_error()
                .data("failed to forward ext method to codex client")));
        }
    }
}

/// Find the currently active thread and turn from gateway state.
///
/// If no turn is active (e.g. the ext_method arrives between turns), falls
/// back to placeholder IDs.
fn find_active_turn(state: &GatewayState) -> (String, String) {
    for (tid, lifecycle) in &state.threads {
        if let SessionLifecycle::InTurn { turn_id, .. } = lifecycle {
            return (tid.clone(), turn_id.clone());
        }
    }
    ("unknown-thread".to_string(), "unknown-turn".to_string())
}

fn active_turn_session(
    state: &GatewayState,
    thread_id: &str,
    expected_turn_id: &str,
) -> Option<String> {
    match state.lifecycle(thread_id) {
        SessionLifecycle::InTurn {
            session_id,
            turn_id,
        } if turn_id == expected_turn_id => Some(session_id.clone()),
        _ => None,
    }
}

/// Process an internal event from a background task (e.g. turn completion).
///
/// Resets lifecycle from `InTurn` → `Idle` so the gateway knows the turn is
/// done and the session is ready for the next prompt.
async fn handle_internal_event(
    event: InternalEvent,
    state: &mut GatewayState,
    outgoing: &OutgoingMessageSender,
) {
    match event {
        InternalEvent::TurnCompleted {
            thread_id,
            turn_id,
            prompt_id,
            outcome,
        } => {
            let succeeded = matches!(outcome, PromptOutcome::Completed { .. });
            debug!(
                %thread_id,
                %turn_id,
                %succeeded,
                "ACP prompt completed"
            );

            if state.active_prompt_ids.get(&turn_id) != Some(&prompt_id) {
                debug!(
                    %thread_id,
                    %turn_id,
                    %prompt_id,
                    "ignoring superseded ACP prompt completion"
                );
                return;
            }

            // Extract the session_id from the current InTurn state.
            let session_id = match active_turn_session(state, &thread_id, &turn_id) {
                Some(session_id) => session_id,
                None => match state.lifecycle(&thread_id) {
                    SessionLifecycle::InTurn {
                        turn_id: active_turn_id,
                        ..
                    } => {
                        warn!(
                            %thread_id,
                            completed_turn_id = %turn_id,
                            %active_turn_id,
                            "ignoring stale turn completion"
                        );
                        return;
                    }
                    other => {
                        warn!(
                            %thread_id,
                            lifecycle = ?other,
                            "TurnCompleted received but lifecycle is not InTurn"
                        );
                        return;
                    }
                },
            };

            let mut output = state
                .turn_outputs
                .remove(&turn_id)
                .unwrap_or_else(|| crate::translation::state::TurnOutput::new(unix_now_millis()));

            if let PromptOutcome::Completed {
                usage: Some(usage), ..
            } = &outcome
            {
                let (method, params) =
                    acp_to_codex::translate_prompt_usage(usage, &thread_id, &turn_id);
                let _ = outgoing.send_notification(method, Some(params)).await;
            }

            let final_agent_message = output.take_agent_message();
            for (method, params) in acp_to_codex::translate_stream_completion(
                &thread_id,
                &turn_id,
                final_agent_message.as_ref(),
                &output.reasoning_summary,
            ) {
                let _ = outgoing.send_notification(method, Some(params)).await;
            }

            match outcome {
                PromptOutcome::Completed { stop_reason, .. } => {
                    let (method, params) = acp_to_codex::translate_prompt_response(
                        &stop_reason,
                        &thread_id,
                        &turn_id,
                        output.started_at_ms,
                    );
                    let _ = outgoing.send_notification(method, Some(params)).await;
                }
                PromptOutcome::Failed(message) => {
                    error!(%thread_id, %turn_id, "ACP prompt failed: {message}");
                    let error_payload = json!({
                        "message": message,
                        "codexErrorInfo": "other",
                        "additionalDetails": null,
                    });
                    let _ = outgoing
                        .send_notification(
                            "error",
                            Some(json!({
                                "threadId": thread_id,
                                "turnId": turn_id,
                                "willRetry": false,
                                "error": error_payload.clone(),
                            })),
                        )
                        .await;
                    let completed_at_ms = unix_now_millis();
                    let _ = outgoing
                        .send_notification(
                            "turn/completed",
                            Some(json!({
                                "threadId": thread_id,
                                "turn": {
                                    "id": turn_id,
                                    "items": [],
                                    "itemsView": "full",
                                    "status": "failed",
                                    "error": error_payload,
                                    "startedAt": output.started_at_ms / 1_000,
                                    "completedAt": completed_at_ms / 1_000,
                                    "durationMs": completed_at_ms
                                        .saturating_sub(output.started_at_ms),
                                },
                            })),
                        )
                        .await;
                }
            }

            // Clean up turn diffs for the completed turn.
            state.turn_diffs.remove(&turn_id);
            state.active_prompt_ids.remove(&turn_id);

            // Transition lifecycle: InTurn → Idle.
            state.set_lifecycle(thread_id.clone(), SessionLifecycle::Idle { session_id });

            // Notify the Codex client that this thread is now idle.
            let _ = outgoing
                .send_notification(
                    "thread/status/changed",
                    Some(json!({
                        "threadId": thread_id,
                        "status": { "type": "idle" },
                    })),
                )
                .await;
        }
    }
}

/// Build a stable turn-level diff without pretending ACP replacement hunks are
/// whole-file snapshots. Each tool retains the diff collection it last
/// reported, and separate edits to the same path remain separate hunks.
fn build_aggregated_diff(diffs: &[crate::translation::state::ToolDiffSet]) -> String {
    let mut output = String::new();
    for tool in diffs {
        for diff in &tool.diffs {
            if !output.is_empty() && !output.ends_with('\n') {
                output.push('\n');
            }
            output.push_str(diff);
        }
    }
    output
}

/// Handle `thread/name/set` — set a user-facing name for a thread.
async fn handle_thread_name_set(
    params: &Value,
    state: &GatewayState,
    outgoing: &OutgoingMessageSender,
) -> Result<Value, GatewayError> {
    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing name".into()))?;

    let tid = codex_protocol::ThreadId::from_string(thread_id)
        .map_err(|_| GatewayError::Translation(format!("invalid threadId: {thread_id}")))?;

    crate::rollout::session_index::append_thread_name(state.rollout.gateway_home(), tid, name)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to set thread name: {e}")))?;

    let _ = outgoing
        .send_notification(
            "thread/name/updated",
            Some(json!({ "threadId": thread_id, "threadName": name })),
        )
        .await;

    Ok(json!({}))
}

/// Handle `thread/archive` — move a rollout file to the archived sessions dir.
async fn handle_thread_archive(
    params: &Value,
    state: &mut GatewayState,
    outgoing: &OutgoingMessageSender,
) -> Result<Value, GatewayError> {
    use crate::rollout::{ARCHIVED_SESSIONS_SUBDIR, list};

    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    let gateway_home = state.rollout.gateway_home().to_path_buf();

    let source_path = list::find_thread_path_by_id_str(&gateway_home, thread_id)
        .await
        .map_err(|e| GatewayError::Translation(format!("archive scan failed: {e}")))?
        .ok_or_else(|| GatewayError::Translation(format!("thread not found: {thread_id}")))?;

    let archive_dir = gateway_home.join(ARCHIVED_SESSIONS_SUBDIR);
    tokio::fs::create_dir_all(&archive_dir)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to create archive dir: {e}")))?;

    let file_name = source_path
        .file_name()
        .ok_or_else(|| GatewayError::Translation("invalid rollout path".into()))?;
    let dest_path = archive_dir.join(file_name);

    tokio::fs::rename(&source_path, &dest_path)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to archive thread: {e}")))?;

    // Close any open writer and update lifecycle.
    state.rollout.close_file(thread_id);
    state.set_lifecycle(thread_id.to_string(), SessionLifecycle::Closed);

    let _ = outgoing
        .send_notification("thread/archived", Some(json!({ "threadId": thread_id })))
        .await;

    Ok(json!({}))
}

/// Handle `thread/unarchive` — move a rollout file back from archived to sessions.
async fn handle_thread_unarchive(
    params: &Value,
    state: &mut GatewayState,
    outgoing: &OutgoingMessageSender,
) -> Result<Value, GatewayError> {
    use crate::rollout::{SESSIONS_SUBDIR, list};

    let thread_id = params
        .get("threadId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| GatewayError::Translation("missing threadId".into()))?;

    let gateway_home = state.rollout.gateway_home().to_path_buf();

    let source_path = list::find_archived_thread_path_by_id_str(&gateway_home, thread_id)
        .await
        .map_err(|e| GatewayError::Translation(format!("unarchive scan failed: {e}")))?
        .ok_or_else(|| {
            GatewayError::Translation(format!("archived thread not found: {thread_id}"))
        })?;

    // Parse YYYY/MM/DD from the filename to reconstruct the directory path.
    let file_name = source_path
        .file_name()
        .ok_or_else(|| GatewayError::Translation("invalid rollout path".into()))?;
    let (year, month, day) = list::rollout_date_parts(file_name).ok_or_else(|| {
        GatewayError::Translation("cannot parse date from rollout filename".into())
    })?;

    let dest_dir = gateway_home
        .join(SESSIONS_SUBDIR)
        .join(&year)
        .join(&month)
        .join(&day);
    tokio::fs::create_dir_all(&dest_dir)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to create sessions dir: {e}")))?;

    let dest_path = dest_dir.join(file_name);
    tokio::fs::rename(&source_path, &dest_path)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to unarchive thread: {e}")))?;

    // Read meta from the restored file for the response.
    let meta = list::read_session_meta_line(&dest_path)
        .await
        .map_err(|e| GatewayError::Translation(format!("failed to read session meta: {e}")))?;

    let now_secs = unix_now_secs();
    let created_secs = parse_rfc3339_to_unix(&meta.meta.timestamp).unwrap_or(now_secs);

    let _ = outgoing
        .send_notification("thread/unarchived", Some(json!({ "threadId": thread_id })))
        .await;

    Ok(json!({
        "thread": {
            "id": thread_id,
            "preview": "",
            "modelProvider": meta.meta.model_provider.as_deref().unwrap_or("acp"),
            "createdAt": created_secs,
            "updatedAt": now_secs,
            "status": { "type": "notLoaded" },
            "cwd": meta.meta.cwd.to_string_lossy(),
            "cliVersion": meta.meta.cli_version,
            "source": meta.meta.source,
            "turns": [],
        },
    }))
}

/// Handle `model/list` — return available models from the agent if known.
///
/// Uses the agent's `SessionModelState` from the last session creation/load
/// response. Falls back to a generic "acp-agent" entry if the agent doesn't
/// advertise model selection.
fn handle_model_list(state: &GatewayState) -> Result<Value, GatewayError> {
    // Check if we have model state from the agent's last session response.
    // The model state is stored when `NewSessionResponse.models` or
    // `LoadSessionResponse.models` is `Some`.
    if let Some(ref model_state) = state.model_state {
        let data: Vec<Value> = model_state
            .available_models
            .iter()
            .map(|m| {
                json!({
                    "id": m.model_id.0.as_ref(),
                    "name": m.name,
                    "description": m.description,
                })
            })
            .collect();
        return Ok(json!({ "data": data, "nextCursor": null }));
    }

    Ok(json!({ "data": [{ "id": "acp-agent", "name": "ACP Agent" }], "nextCursor": null }))
}

// ─── Helpers ──────────────────────────────────────────────────────────

/// Build a Thread JSON object from a `list::ThreadItem` for `thread/list`.
fn build_thread_json_from_list_item(
    item: &crate::rollout::list::ThreadItem,
    name: Option<String>,
    threads: &std::collections::HashMap<String, SessionLifecycle>,
) -> Value {
    let thread_id_str = item.thread_id.map(|id| id.to_string()).unwrap_or_default();
    let status = lifecycle_to_thread_status(
        threads
            .get(&thread_id_str)
            .unwrap_or(&SessionLifecycle::Uninitialized),
    );
    let created_secs = item
        .created_at
        .as_deref()
        .and_then(parse_rfc3339_to_unix)
        .unwrap_or(0);
    let updated_secs = item
        .updated_at
        .as_deref()
        .and_then(parse_rfc3339_to_unix)
        .unwrap_or(created_secs);

    json!({
        "id": thread_id_str,
        "preview": item.first_user_message.as_deref().unwrap_or(""),
        "modelProvider": item.model_provider.as_deref().unwrap_or("acp"),
        "createdAt": created_secs,
        "updatedAt": updated_secs,
        "status": status,
        "cwd": item.cwd.as_ref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
        "cliVersion": item.cli_version.as_deref().unwrap_or(""),
        "source": item.source.as_ref().unwrap_or(&SessionSource::VSCode),
        "name": name,
        "turns": [],
    })
}

/// Map `SessionLifecycle` to a Codex `ThreadStatus` JSON value.
fn lifecycle_to_thread_status(lifecycle: &SessionLifecycle) -> Value {
    match lifecycle {
        SessionLifecycle::Idle { .. } => json!({ "type": "idle" }),
        SessionLifecycle::InTurn { .. } => {
            json!({ "type": "active", "activeFlags": ["agentTurn"] })
        }
        SessionLifecycle::Closed => json!({ "type": "notLoaded" }),
        _ => json!({ "type": "notLoaded" }),
    }
}

/// Return current unix timestamp in seconds.
fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Return current unix timestamp in milliseconds.
fn unix_now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Parse an RFC 3339 timestamp string to unix seconds.
fn parse_rfc3339_to_unix(s: &str) -> Option<i64> {
    OffsetDateTime::parse(s, &Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct CancelRecordingAgent {
        cancelled_sessions: RefCell<Vec<String>>,
    }

    #[async_trait::async_trait(?Send)]
    impl Agent for CancelRecordingAgent {
        async fn initialize(
            &self,
            _args: agent_client_protocol::InitializeRequest,
        ) -> agent_client_protocol::Result<agent_client_protocol::InitializeResponse> {
            Err(agent_client_protocol::Error::method_not_found())
        }

        async fn authenticate(
            &self,
            _args: agent_client_protocol::AuthenticateRequest,
        ) -> agent_client_protocol::Result<agent_client_protocol::AuthenticateResponse> {
            Err(agent_client_protocol::Error::method_not_found())
        }

        async fn new_session(
            &self,
            _args: agent_client_protocol::NewSessionRequest,
        ) -> agent_client_protocol::Result<agent_client_protocol::NewSessionResponse> {
            Err(agent_client_protocol::Error::method_not_found())
        }

        async fn prompt(
            &self,
            _args: agent_client_protocol::PromptRequest,
        ) -> agent_client_protocol::Result<agent_client_protocol::PromptResponse> {
            Err(agent_client_protocol::Error::method_not_found())
        }

        async fn cancel(
            &self,
            args: agent_client_protocol::CancelNotification,
        ) -> agent_client_protocol::Result<()> {
            self.cancelled_sessions
                .borrow_mut()
                .push(args.session_id.to_string());
            Ok(())
        }
    }

    #[test]
    fn steer_prepares_unique_send_now_prompts_without_a_pending_queue() {
        let mut state = GatewayState::new();
        state.set_lifecycle(
            "thread".into(),
            SessionLifecycle::InTurn {
                session_id: "session".into(),
                turn_id: "turn".into(),
            },
        );
        let mut ids = IdMap::new();
        ids.create_thread_session_mapping("thread".into(), "session".into());
        let params = json!({
            "threadId": "thread",
            "expectedTurnId": "turn",
            "input": [{"type": "text", "text": "new direction", "textElements": []}],
        });

        let (thread_id, turn_id, first_prompt_id, first_request) =
            prepare_turn_steer(&params, &mut state, &ids).unwrap();
        let (_, _, second_prompt_id, second_request) =
            prepare_turn_steer(&params, &mut state, &ids).unwrap();

        assert_eq!(thread_id, "thread");
        assert_eq!(turn_id, "turn");
        assert_ne!(first_prompt_id, second_prompt_id);
        assert_eq!(first_request.meta.as_ref().unwrap()["sendNow"], true);
        assert_eq!(second_request.meta.as_ref().unwrap()["sendNow"], true);
        assert_eq!(state.active_prompt_ids.get("turn"), Some(&second_prompt_id));
        assert!(matches!(
            state.lifecycle("thread"),
            SessionLifecycle::InTurn { turn_id, .. } if turn_id == "turn"
        ));
    }

    #[test]
    fn rejects_a_second_turn_and_stale_completion_identity() {
        let mut state = GatewayState::new();
        state.set_lifecycle(
            "thread".into(),
            SessionLifecycle::InTurn {
                session_id: "session".into(),
                turn_id: "active-turn".into(),
            },
        );

        let error = validate_turn_start_lifecycle(&state, "thread", "session").unwrap_err();
        assert!(error.to_string().contains("active-turn"));
        assert_eq!(
            active_turn_session(&state, "thread", "active-turn").as_deref(),
            Some("session")
        );
        assert_eq!(active_turn_session(&state, "thread", "stale-turn"), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn superseded_prompt_completion_cannot_end_the_codex_turn() {
        let mut state = GatewayState::new();
        state.set_lifecycle(
            "thread".into(),
            SessionLifecycle::InTurn {
                session_id: "session".into(),
                turn_id: "turn".into(),
            },
        );
        state.turn_outputs.insert(
            "turn".into(),
            crate::translation::state::TurnOutput::new(1_000),
        );
        state
            .active_prompt_ids
            .insert("turn".into(), "new-prompt".into());
        let (outbound_tx, mut outbound_rx) = mpsc::channel(8);
        let outgoing = OutgoingMessageSender::new(outbound_tx);

        handle_internal_event(
            InternalEvent::TurnCompleted {
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                prompt_id: "old-prompt".into(),
                outcome: PromptOutcome::Completed {
                    stop_reason: "Cancelled".into(),
                    usage: None,
                },
            },
            &mut state,
            &outgoing,
        )
        .await;

        assert!(matches!(
            state.lifecycle("thread"),
            SessionLifecycle::InTurn { turn_id, .. } if turn_id == "turn"
        ));
        assert!(state.turn_outputs.contains_key("turn"));
        assert_eq!(
            state.active_prompt_ids.get("turn").map(String::as_str),
            Some("new-prompt")
        );
        assert!(matches!(
            outbound_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn interrupt_rejects_a_stale_turn_id_without_cancelling() {
        let agent = CancelRecordingAgent {
            cancelled_sessions: RefCell::new(Vec::new()),
        };
        let mut state = GatewayState::new();
        state.set_lifecycle(
            "thread".into(),
            SessionLifecycle::InTurn {
                session_id: "session".into(),
                turn_id: "new-turn".into(),
            },
        );
        let mut ids = IdMap::new();
        ids.create_thread_session_mapping("thread".into(), "session".into());

        let result = handle_turn_interrupt(
            &json!({"threadId":"thread","turnId":"old-turn"}),
            &agent,
            &state,
            &ids,
        )
        .await;

        assert!(result.is_err());
        assert!(agent.cancelled_sessions.borrow().is_empty());
    }
}
