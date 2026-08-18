//! Hybrid Codex app-server multiplexer.
//!
//! Native traffic is sent to the bundled Codex CLI. Grok traffic is sent to
//! this binary's Codex-to-ACP mode, which owns the ACP translation and Grok
//! subprocess. The multiplexer only handles routing, aggregation, and the
//! minimal namespacing needed to keep backend ownership unambiguous.

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const BRIDGE_PREFIX: &str = "grok/";
const GROK_MODEL: &str = "grok-4.6";
const INTERNAL_ID_PREFIX: &str = "__codex_grok_bridge_client_";
const SERVER_ID_PREFIX: &str = "__codex_grok_bridge_server_";
const THREAD_LIST_BATCH_LIMIT: u64 = 1000;
const SUPPORTED_CODEX_PREFIX: &str = "codex-cli 0.148.";
const SUPPORTED_GROK_PREFIX: &str = "grok 1.0.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Backend {
    Native,
    Grok,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RouteRecord {
    backend: Backend,
    model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    acp_session_id: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RouteFile {
    version: u32,
    threads: HashMap<String, RouteRecord>,
}

struct RouteStore {
    path: PathBuf,
    file: RouteFile,
}

impl RouteStore {
    fn load(path: PathBuf) -> Result<Self> {
        let file = match std::fs::read_to_string(&path) {
            Ok(contents) => serde_json::from_str(&contents)
                .with_context(|| format!("invalid routing file {}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => RouteFile {
                version: 1,
                threads: HashMap::new(),
            },
            Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
        };
        Ok(Self { path, file })
    }

    fn backend(&self, thread_id: &str) -> Option<Backend> {
        self.file
            .threads
            .get(thread_id)
            .map(|record| record.backend)
    }

    fn public_thread_id_for_acp(&self, acp_session_id: &str) -> Option<&str> {
        self.file
            .threads
            .iter()
            .find(|(thread_id, record)| {
                record.backend == Backend::Grok
                    && thread_id.as_str() != acp_session_id
                    && record.acp_session_id.as_deref() == Some(acp_session_id)
            })
            .map(|(thread_id, _)| thread_id.as_str())
            .or_else(|| {
                self.file
                    .threads
                    .get_key_value(acp_session_id)
                    .filter(|(_, record)| record.backend == Backend::Grok)
                    .map(|(thread_id, _)| thread_id.as_str())
            })
    }

    fn insert(&mut self, thread_id: String, record: RouteRecord) -> Result<()> {
        self.file.threads.insert(thread_id, record);
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| anyhow!("routing file has no parent"))?;
        std::fs::create_dir_all(parent)?;
        let temporary = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&self.file)?;
        std::fs::write(&temporary, bytes)?;
        std::fs::rename(&temporary, &self.path)?;
        Ok(())
    }
}

#[derive(Debug)]
enum ChildEvent {
    Line(Backend, String),
    Exited(Backend, Option<i32>),
}

struct ChildPeer {
    child: Child,
    stdin: ChildStdin,
}

impl ChildPeer {
    async fn send_line(&mut self, line: &str) -> Result<()> {
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }
}

#[derive(Debug)]
enum PendingKind {
    Pass {
        backend: Backend,
        client_id: Value,
        method: String,
    },
    Aggregate {
        group_id: u64,
        backend: Backend,
    },
}

#[derive(Debug, Clone, Copy)]
enum AggregateKind {
    Initialize,
    Models,
    Threads,
}

#[derive(Debug)]
struct Aggregate {
    kind: AggregateKind,
    client_id: Value,
    params: Value,
    native: Option<Value>,
    grok: Option<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct BackendThreadCursor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ThreadCursorCheckpoint {
    native: BackendThreadCursor,
    grok: BackendThreadCursor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompositeThreadCursor {
    version: u8,
    #[serde(default)]
    native: BackendThreadCursor,
    #[serde(default)]
    grok: BackendThreadCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    history: Vec<ThreadCursorCheckpoint>,
}

impl Default for CompositeThreadCursor {
    fn default() -> Self {
        Self {
            version: 1,
            native: BackendThreadCursor::default(),
            grok: BackendThreadCursor::default(),
            history: Vec::new(),
        }
    }
}

impl CompositeThreadCursor {
    fn backend(&self, backend: Backend) -> &BackendThreadCursor {
        match backend {
            Backend::Native => &self.native,
            Backend::Grok => &self.grok,
        }
    }

    fn backend_mut(&mut self, backend: Backend) -> &mut BackendThreadCursor {
        match backend {
            Backend::Native => &mut self.native,
            Backend::Grok => &mut self.grok,
        }
    }

    fn has_more(&self) -> bool {
        !self.native.exhausted || !self.grok.exhausted
    }
}

/// Entry point used when the desktop invokes the bridge through CODEX_CLI_PATH.
pub async fn run_from_codex_cli_args(cli_args: &[OsString]) -> Result<()> {
    let native_path = native_codex_path();
    let grok_path = grok_path();
    verify_versions(&native_path, &grok_path).await?;

    let current_exe = std::env::current_exe().context("resolve bridge executable")?;
    let cwd = std::env::current_dir().context("resolve current directory")?;

    let app_server_index = cli_args
        .iter()
        .position(|arg| arg == "app-server")
        .ok_or_else(|| anyhow!("missing app-server subcommand"))?;
    let native_args: Vec<OsString> = cli_args.to_vec();

    let (events_tx, mut events_rx) = mpsc::channel(512);
    let mut native = spawn_peer(
        Backend::Native,
        &native_path,
        &native_args,
        &cwd,
        events_tx.clone(),
        true,
    )
    .await?;

    let grok_args = vec![
        OsString::from("--log-level"),
        OsString::from("warn"),
        OsString::from("--agent-cmd"),
        grok_path.clone().into_os_string(),
        OsString::from("--"),
        OsString::from("agent"),
        OsString::from("stdio"),
    ];
    let mut grok = spawn_peer(
        Backend::Grok,
        &current_exe,
        &grok_args,
        &cwd,
        events_tx,
        false,
    )
    .await?;

    debug!(app_server_index, "hybrid children started");

    let (client_tx, mut client_rx) = mpsc::channel::<String>(512);
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if client_tx.send(line).await.is_err() {
                break;
            }
        }
    });

    let state_path = state_path();
    let mut routes = RouteStore::load(state_path)?;
    let mut pending: HashMap<String, PendingKind> = HashMap::new();
    let mut aggregates: HashMap<u64, Aggregate> = HashMap::new();
    let mut grok_server_ids: HashMap<String, Value> = HashMap::new();
    let mut next_id = 1_u64;
    let mut next_server_id = 1_u64;
    let mut stdout = tokio::io::stdout();
    loop {
        tokio::select! {
            client_line = client_rx.recv() => {
                let Some(line) = client_line else {
                    break;
                };
                let message: Value = match serde_json::from_str(&line) {
                    Ok(message) => message,
                    Err(error) => {
                        write_json(&mut stdout, &jsonrpc_error(Value::Null, -32700, format!("invalid JSON: {error}"))).await?;
                        continue;
                    }
                };
                handle_client_message(
                    message,
                    &line,
                    &mut native,
                    &mut grok,
                    &mut routes,
                    &mut pending,
                    &mut aggregates,
                    &mut grok_server_ids,
                    &mut next_id,
                    &mut stdout,
                ).await?;
            }
            child_event = events_rx.recv() => {
                let Some(child_event) = child_event else { break };
                match child_event {
                    ChildEvent::Line(backend, line) => {
                        handle_child_line(
                            backend,
                            line,
                            &mut routes,
                            &mut pending,
                            &mut aggregates,
                            &mut grok_server_ids,
                            &mut next_server_id,
                            &mut stdout,
                        ).await?;
                    }
                    ChildEvent::Exited(backend, code) => {
                        bail!("{backend:?} child exited unexpectedly with {code:?}");
                    }
                }
            }
        }
    }

    let _ = native.child.start_kill();
    let _ = grok.child.start_kill();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_message(
    mut message: Value,
    original_line: &str,
    native: &mut ChildPeer,
    grok: &mut ChildPeer,
    routes: &mut RouteStore,
    pending: &mut HashMap<String, PendingKind>,
    aggregates: &mut HashMap<u64, Aggregate>,
    grok_server_ids: &mut HashMap<String, Value>,
    next_id: &mut u64,
    stdout: &mut tokio::io::Stdout,
) -> Result<()> {
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let id = message.get("id").cloned();

    // Responses to native server requests retain their IDs. Grok server
    // requests are namespaced so simultaneous requests cannot collide.
    if method.is_none() {
        if let Some(id_string) = id.as_ref().and_then(Value::as_str)
            && id_string.starts_with(SERVER_ID_PREFIX)
            && let Some(original_id) = grok_server_ids.remove(id_string)
        {
            message["id"] = original_id;
            grok.send_line(&serde_json::to_string(&message)?).await?;
        } else {
            native.send_line(original_line).await?;
        }
        return Ok(());
    }

    let method = method.expect("checked above");
    if id.is_none() {
        // Initialization acknowledgements and client notifications go to both
        // runtimes. Backend-specific notifications use the thread route.
        if let Some(thread_id) = thread_id(&message) {
            match routes.backend(thread_id).unwrap_or(Backend::Native) {
                Backend::Native => native.send_line(original_line).await?,
                Backend::Grok => grok.send_line(original_line).await?,
            }
        } else {
            native.send_line(original_line).await?;
            grok.send_line(original_line).await?;
        }
        return Ok(());
    }

    let client_id = id.expect("checked above");
    let aggregate_kind = match method.as_str() {
        "initialize" => Some(AggregateKind::Initialize),
        "model/list" => Some(AggregateKind::Models),
        "thread/list" => Some(AggregateKind::Threads),
        _ => None,
    };

    if let Some(kind) = aggregate_kind {
        let thread_cursor = if matches!(kind, AggregateKind::Threads) {
            match decode_thread_cursor(params_cursor(&message)) {
                Ok(cursor) => Some(cursor),
                Err(error) => {
                    write_json(stdout, &jsonrpc_error(client_id, -32602, error.to_string()))
                        .await?;
                    return Ok(());
                }
            }
        } else {
            None
        };
        let group_id = *next_id;
        *next_id += 1;
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        aggregates.insert(
            group_id,
            Aggregate {
                kind,
                client_id,
                params,
                native: None,
                grok: None,
            },
        );
        for (backend, peer) in [(Backend::Native, native), (Backend::Grok, grok)] {
            if thread_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.backend(backend).exhausted)
            {
                let aggregate = aggregates.get_mut(&group_id).expect("just inserted");
                let empty = json!({"result":{"data":[],"nextCursor":null}});
                match backend {
                    Backend::Native => aggregate.native = Some(empty),
                    Backend::Grok => aggregate.grok = Some(empty),
                }
                continue;
            }

            let mut child_message = message.clone();
            match kind {
                AggregateKind::Models => {
                    child_message["params"]["cursor"] = Value::Null;
                    child_message["params"]["limit"] =
                        Value::Number(THREAD_LIST_BATCH_LIMIT.into());
                }
                AggregateKind::Threads => {
                    let backend_cursor = thread_cursor
                        .as_ref()
                        .expect("decoded above")
                        .backend(backend);
                    child_message["params"]["cursor"] = backend_cursor
                        .cursor
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null);
                    child_message["params"]["limit"] =
                        Value::Number(THREAD_LIST_BATCH_LIMIT.into());
                }
                AggregateKind::Initialize => {}
            }
            let internal_id = new_internal_id(next_id);
            child_message["id"] = Value::String(internal_id.clone());
            pending.insert(internal_id, PendingKind::Aggregate { group_id, backend });
            peer.send_line(&serde_json::to_string(&child_message)?)
                .await?;
        }
        let complete = aggregates
            .get(&group_id)
            .is_some_and(|aggregate| aggregate.native.is_some() && aggregate.grok.is_some());
        if complete {
            let aggregate = aggregates.remove(&group_id).expect("present");
            let response = finish_aggregate(aggregate, routes)?;
            write_json(stdout, &response).await?;
        }
        return Ok(());
    }

    let backend = route_request(&message, routes)?;
    if let Err(error) = validate_model_boundary(&message, backend) {
        write_json(stdout, &jsonrpc_error(client_id, -32602, error.to_string())).await?;
        return Ok(());
    }

    // Native task traffic is transparent JSONL, including thread creation.
    // Unknown thread IDs already default to native, so the bridge does not
    // need to correlate or persist native ownership. This keeps rollouts made
    // with OpenAI models identical to rollouts made by vanilla Codex.
    if let Some(line) = native_passthrough_line(backend, original_line) {
        native.send_line(line).await?;
        return Ok(());
    }

    strip_grok_model_namespace(&mut message);
    let internal_id = new_internal_id(next_id);
    message["id"] = Value::String(internal_id.clone());
    pending.insert(
        internal_id,
        PendingKind::Pass {
            backend,
            client_id,
            method,
        },
    );
    grok.send_line(&serde_json::to_string(&message)?).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_child_line(
    backend: Backend,
    line: String,
    routes: &mut RouteStore,
    pending: &mut HashMap<String, PendingKind>,
    aggregates: &mut HashMap<u64, Aggregate>,
    grok_server_ids: &mut HashMap<String, Value>,
    next_server_id: &mut u64,
    stdout: &mut tokio::io::Stdout,
) -> Result<()> {
    let mut message: Value =
        serde_json::from_str(&line).with_context(|| format!("{backend:?} emitted invalid JSON"))?;

    if let Some(id_string) = message.get("id").and_then(Value::as_str)
        && let Some(pending_kind) = pending.remove(id_string)
    {
        match pending_kind {
            PendingKind::Pass {
                backend,
                client_id,
                method,
            } => {
                message["id"] = client_id;
                if backend == Backend::Grok {
                    normalize_grok_message(&mut message);
                }
                record_route_from_response(routes, backend, &method, &message)?;
                write_json(stdout, &message).await?;
            }
            PendingKind::Aggregate { group_id, backend } => {
                let aggregate = aggregates
                    .get_mut(&group_id)
                    .ok_or_else(|| anyhow!("missing aggregate {group_id}"))?;
                match backend {
                    Backend::Native => aggregate.native = Some(message),
                    Backend::Grok => aggregate.grok = Some(message),
                }
                if aggregate.native.is_some() && aggregate.grok.is_some() {
                    let aggregate = aggregates.remove(&group_id).expect("present");
                    let response = finish_aggregate(aggregate, routes)?;
                    write_json(stdout, &response).await?;
                }
            }
        }
        return Ok(());
    }

    if backend == Backend::Grok {
        if message
            .get("method")
            .and_then(Value::as_str)
            .is_some_and(|m| m.starts_with("gateway/"))
        {
            return Ok(());
        }
        if message.get("method").is_some() && message.get("id").is_some() {
            let original_id = message["id"].clone();
            let namespaced = new_server_id(next_server_id);
            grok_server_ids.insert(namespaced.clone(), original_id);
            message["id"] = Value::String(namespaced);
        }
        normalize_grok_message(&mut message);
        write_json(stdout, &message).await?;
    } else {
        stdout.write_all(line.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}

fn finish_aggregate(mut aggregate: Aggregate, routes: &mut RouteStore) -> Result<Value> {
    let native = aggregate.native.take().expect("checked");
    let grok = aggregate.grok.take().expect("checked");
    if let Some(error) = native.get("error") {
        return Ok(json!({ "id": aggregate.client_id, "error": error }));
    }
    if let Some(error) = grok.get("error") {
        return Ok(jsonrpc_error(
            aggregate.client_id,
            -32001,
            format!(
                "Grok backend initialization failed: {}",
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ),
        ));
    }

    let result = match aggregate.kind {
        AggregateKind::Initialize => native.get("result").cloned().unwrap_or_else(|| json!({})),
        AggregateKind::Models => merge_models(&native, &aggregate.params),
        AggregateKind::Threads => merge_threads(&native, &grok, &aggregate.params, routes)?,
    };
    Ok(json!({ "id": aggregate.client_id, "result": result }))
}

fn merge_models(native: &Value, params: &Value) -> Value {
    let mut data = native
        .pointer("/result/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    data.push(grok_model());
    let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(100) as usize;
    let offset =
        decode_offset_cursor(params.get("cursor").and_then(Value::as_str), "models").unwrap_or(0);
    let end = (offset + limit).min(data.len());
    let page = if offset < data.len() {
        data[offset..end].to_vec()
    } else {
        Vec::new()
    };
    let next_cursor = (end < data.len()).then(|| encode_offset_cursor("models", end));
    json!({ "data": page, "nextCursor": next_cursor })
}

fn merge_threads(
    native: &Value,
    grok: &Value,
    params: &Value,
    routes: &mut RouteStore,
) -> Result<Value> {
    let mut cursor = decode_thread_cursor(params.get("cursor").and_then(Value::as_str))?;
    let native_data = native
        .pointer("/result/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut grok_data = Vec::new();
    for mut thread in grok
        .pointer("/result/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        normalize_thread(&mut thread);
        if let Some(acp_session_id) = thread.get("id").and_then(Value::as_str).map(str::to_owned) {
            if let Some(public_thread_id) = routes
                .public_thread_id_for_acp(&acp_session_id)
                .map(str::to_owned)
            {
                thread["id"] = Value::String(public_thread_id.clone());
                thread["sessionId"] = Value::String(public_thread_id);
            } else {
                routes.insert(
                    acp_session_id.clone(),
                    RouteRecord {
                        backend: Backend::Grok,
                        model: format!("{BRIDGE_PREFIX}{GROK_MODEL}"),
                        // Agent-only sessions have no bridge-generated Codex
                        // ID, so their ACP session ID is the public identity.
                        acp_session_id: Some(acp_session_id),
                    },
                )?;
            }
        }
        grok_data.push(thread);
    }

    let native_offset = cursor.native.offset;
    let grok_offset = cursor.grok.offset;
    if native_offset > native_data.len() || grok_offset > grok_data.len() {
        bail!("thread cursor offset exceeds the backend page length");
    }

    let mut data: Vec<(Backend, Value)> = native_data
        .iter()
        .skip(native_offset)
        .cloned()
        .map(|thread| (Backend::Native, thread))
        .chain(
            grok_data
                .iter()
                .skip(grok_offset)
                .cloned()
                .map(|thread| (Backend::Grok, thread)),
        )
        .collect();

    let sort_key = params
        .get("sortKey")
        .and_then(Value::as_str)
        .unwrap_or("created_at");
    let field = if sort_key == "updated_at" {
        "updatedAt"
    } else {
        "createdAt"
    };
    let descending = params
        .get("sortDirection")
        .and_then(Value::as_str)
        .unwrap_or("desc")
        != "asc";
    data.sort_by(|(_, left), (_, right)| {
        let left_key = left.get(field).and_then(Value::as_i64).unwrap_or_default();
        let right_key = right.get(field).and_then(Value::as_i64).unwrap_or_default();
        let order = left_key.cmp(&right_key).then_with(|| {
            left.get("id")
                .and_then(Value::as_str)
                .cmp(&right.get("id").and_then(Value::as_str))
        });
        if descending { order.reverse() } else { order }
    });

    let limit = params.get("limit").and_then(Value::as_u64).unwrap_or(20) as usize;
    let selected: Vec<(Backend, Value)> = data.into_iter().take(limit).collect();
    let native_consumed = selected
        .iter()
        .filter(|(backend, _)| *backend == Backend::Native)
        .count();
    let grok_consumed = selected.len().saturating_sub(native_consumed);

    let backwards_cursor = cursor.history.last().map(|checkpoint| {
        let mut previous = cursor.clone();
        previous.native = checkpoint.native.clone();
        previous.grok = checkpoint.grok.clone();
        previous.history.pop();
        encode_thread_cursor(&previous)
    });
    let checkpoint = ThreadCursorCheckpoint {
        native: cursor.native.clone(),
        grok: cursor.grok.clone(),
    };

    advance_thread_cursor(
        cursor.backend_mut(Backend::Native),
        native,
        native_data.len(),
        native_consumed,
    )?;
    advance_thread_cursor(
        cursor.backend_mut(Backend::Grok),
        grok,
        grok_data.len(),
        grok_consumed,
    )?;

    if cursor.native != checkpoint.native || cursor.grok != checkpoint.grok {
        cursor.history.push(checkpoint);
    }

    let next_cursor = cursor.has_more().then(|| encode_thread_cursor(&cursor));
    let page: Vec<Value> = selected.into_iter().map(|(_, thread)| thread).collect();
    Ok(json!({ "data": page, "nextCursor": next_cursor, "backwardsCursor": backwards_cursor }))
}

fn advance_thread_cursor(
    cursor: &mut BackendThreadCursor,
    response: &Value,
    page_len: usize,
    consumed: usize,
) -> Result<()> {
    let offset = cursor.offset.saturating_add(consumed);
    if offset < page_len {
        cursor.offset = offset;
        return Ok(());
    }

    cursor.offset = 0;
    cursor.cursor = match response.pointer("/result/nextCursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => bail!("backend returned a non-string thread cursor"),
    };
    cursor.exhausted = cursor.cursor.is_none();
    Ok(())
}

fn route_request(message: &Value, routes: &RouteStore) -> Result<Backend> {
    if message.get("method").and_then(Value::as_str) == Some("thread/start") {
        return Ok(if selected_model(message).is_some_and(is_grok_model) {
            Backend::Grok
        } else {
            Backend::Native
        });
    }
    if let Some(thread_id) = thread_id(message) {
        return Ok(routes.backend(thread_id).unwrap_or(Backend::Native));
    }
    Ok(Backend::Native)
}

fn native_passthrough_line(backend: Backend, original_line: &str) -> Option<&str> {
    (backend == Backend::Native).then_some(original_line)
}

fn validate_model_boundary(message: &Value, backend: Backend) -> Result<()> {
    let Some(model) = selected_model(message) else {
        return Ok(());
    };
    match (backend, is_grok_model(model)) {
        (Backend::Grok, false) => bail!(
            "cross-backend model change rejected: Grok thread cannot use native model {model:?}"
        ),
        (Backend::Native, true)
            if message.get("method").and_then(Value::as_str) != Some("thread/start") =>
        {
            bail!(
                "cross-backend model change rejected: native thread cannot use Grok model {model:?}"
            )
        }
        _ => Ok(()),
    }
}

fn record_route_from_response(
    routes: &mut RouteStore,
    backend: Backend,
    method: &str,
    message: &Value,
) -> Result<()> {
    if backend != Backend::Grok
        || !matches!(method, "thread/start" | "thread/resume")
        || message.get("error").is_some()
    {
        return Ok(());
    }
    let Some(thread_id) = message.pointer("/result/thread/id").and_then(Value::as_str) else {
        return Ok(());
    };
    let acp_session_id = message
        .pointer("/result/_meta/grokBridge/acpSessionId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    routes.insert(
        thread_id.to_string(),
        RouteRecord {
            backend,
            model: format!("{BRIDGE_PREFIX}{GROK_MODEL}"),
            acp_session_id,
        },
    )
}

fn normalize_grok_message(message: &mut Value) {
    if message.pointer("/result/thread").is_some()
        && let Some(result) = message.get_mut("result").and_then(Value::as_object_mut)
    {
        result.insert(
            "model".into(),
            Value::String(format!("{BRIDGE_PREFIX}{GROK_MODEL}")),
        );
        result.insert("modelProvider".into(), Value::String("grok".into()));
        if result.get("reasoningEffort").is_none_or(Value::is_null) {
            result.insert("reasoningEffort".into(), Value::String("high".into()));
        }
    }
    if let Some(thread) = message.pointer_mut("/result/thread") {
        normalize_thread(thread);
    }
    if let Some(thread) = message.pointer_mut("/params/thread") {
        normalize_thread(thread);
    }
    if let Some(turn) = message.pointer_mut("/result/turn") {
        normalize_turn(turn);
    }
    if let Some(turn) = message.pointer_mut("/params/turn") {
        normalize_turn(turn);
    }
    if let Some(item) = message.pointer_mut("/params/item") {
        normalize_item(item);
    }
    if let Some(method) = message
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned)
    {
        let timestamp_key = match method.as_str() {
            "item/started" => Some("startedAtMs"),
            "item/completed" => Some("completedAtMs"),
            _ => None,
        };
        if let Some(timestamp_key) = timestamp_key
            && let Some(params) = message.get_mut("params").and_then(Value::as_object_mut)
        {
            insert_default(
                params,
                timestamp_key,
                Value::Number((unix_now() * 1000).into()),
            );
        }
        if method == "item/commandExecution/requestApproval"
            && let Some(params) = message.get_mut("params").and_then(Value::as_object_mut)
        {
            insert_default(
                params,
                "startedAtMs",
                Value::Number((unix_now() * 1000).into()),
            );
            insert_default(params, "approvalId", Value::Null);
            insert_default(params, "environmentId", Value::Null);
            insert_default(params, "networkApprovalContext", Value::Null);
            insert_default(params, "commandActions", json!([]));
            insert_default(params, "additionalPermissions", Value::Null);
            insert_default(params, "proposedExecpolicyAmendment", Value::Null);
            insert_default(params, "proposedNetworkPolicyAmendments", Value::Null);
            insert_default(
                params,
                "availableDecisions",
                json!(["accept", "acceptForSession", "decline", "cancel"]),
            );
        }
        if method == "item/fileChange/requestApproval"
            && let Some(params) = message.get_mut("params").and_then(Value::as_object_mut)
        {
            insert_default(
                params,
                "startedAtMs",
                Value::Number((unix_now() * 1000).into()),
            );
        }
    }
    if let Some(token_usage) = message.pointer_mut("/params/tokenUsage") {
        for key in ["total", "last"] {
            if let Some(breakdown) = token_usage.get_mut(key).and_then(Value::as_object_mut) {
                insert_default(breakdown, "cacheWriteInputTokens", Value::Number(0.into()));
            }
        }
    }
}

fn normalize_thread(thread: &mut Value) {
    let Some(object) = thread.as_object_mut() else {
        return;
    };
    let now = unix_now();
    normalize_timestamp(object, "createdAt", now);
    normalize_timestamp(object, "updatedAt", now);
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    insert_default(object, "extra", Value::Null);
    insert_default(object, "sessionId", Value::String(id));
    insert_default(object, "forkedFromId", Value::Null);
    insert_default(object, "parentThreadId", Value::Null);
    insert_default(object, "ephemeral", Value::Bool(false));
    insert_default(object, "section", Value::Null);
    insert_default(object, "sectionEnteredAt", Value::Null);
    insert_default(object, "historyMode", Value::String("full".into()));
    object.insert("modelProvider".into(), Value::String("grok".into()));
    insert_default(
        object,
        "recencyAt",
        object.get("updatedAt").cloned().unwrap_or(Value::Null),
    );
    insert_default(object, "path", Value::Null);
    insert_default(object, "canAcceptDirectInput", Value::Bool(true));
    insert_default(object, "threadSource", Value::Null);
    insert_default(object, "agentNickname", Value::Null);
    insert_default(object, "agentRole", Value::Null);
    insert_default(object, "gitInfo", Value::Null);
    insert_default(object, "name", Value::Null);
}

fn normalize_turn(turn: &mut Value) {
    let Some(object) = turn.as_object_mut() else {
        return;
    };
    insert_default(object, "itemsView", Value::String("full".into()));
    insert_default(object, "startedAt", Value::Number(unix_now().into()));
    insert_default(object, "completedAt", Value::Null);
    insert_default(object, "durationMs", Value::Null);
    if let Some(error) = object.get_mut("error").and_then(Value::as_object_mut) {
        insert_default(error, "codexErrorInfo", Value::String("other".into()));
        insert_default(error, "additionalDetails", Value::Null);
    }
}

fn normalize_item(item: &mut Value) {
    let Some(object) = item.as_object_mut() else {
        return;
    };
    match object.get("type").and_then(Value::as_str) {
        Some("agentMessage") => {
            insert_default(object, "phase", Value::Null);
            insert_default(object, "memoryCitation", Value::Null);
        }
        Some("reasoning") => {
            insert_default(object, "summary", json!([]));
            insert_default(object, "content", json!([]));
        }
        Some("commandExecution") => {
            insert_default(object, "pluginId", Value::Null);
            insert_default(object, "scriptPath", Value::Null);
            insert_default(object, "processId", Value::Null);
            insert_default(object, "source", Value::String("agent".into()));
            insert_default(object, "commandActions", json!([]));
            insert_default(object, "aggregatedOutput", Value::Null);
            insert_default(object, "exitCode", Value::Null);
            insert_default(object, "durationMs", Value::Null);
        }
        Some("mcpToolCall") => {
            insert_default(object, "appContext", Value::Null);
            insert_default(object, "pluginId", Value::Null);
            insert_default(object, "readOnlyHint", Value::Null);
            insert_default(object, "result", Value::Null);
            insert_default(object, "error", Value::Null);
            insert_default(object, "durationMs", Value::Null);
        }
        _ => {}
    }
}

fn grok_model() -> Value {
    json!({
        "id": format!("{BRIDGE_PREFIX}{GROK_MODEL}"),
        "model": format!("{BRIDGE_PREFIX}{GROK_MODEL}"),
        "upgrade": null,
        "upgradeInfo": null,
        "availabilityNux": null,
        "displayName": "Grok 4.6",
        "description": "SpaceXAI's latest frontier model via Grok Build",
        "modelSpecialty": null,
        "hidden": false,
        "supportedReasoningEfforts": [
            { "reasoningEffort": "xhigh", "description": "Highest effort and reasoning level" },
            { "reasoningEffort": "high", "description": "Higher implementation quality with extensive reasoning" },
            { "reasoningEffort": "medium", "description": "Balanced effort with standard implementation and testing" },
            { "reasoningEffort": "low", "description": "Quick, fast implementations" }
        ],
        "defaultReasoningEffort": "high",
        "inputModalities": ["text"],
        "supportsPersonality": false,
        "multiAgentVersion": null,
        "additionalSpeedTiers": [],
        "serviceTiers": [],
        "defaultServiceTier": null,
        "isDefault": false
    })
}

fn strip_grok_model_namespace(message: &mut Value) {
    for pointer in ["/params/model", "/params/config/model"] {
        if let Some(model) = message.pointer_mut(pointer)
            && let Some(raw) = model
                .as_str()
                .and_then(|value| value.strip_prefix(BRIDGE_PREFIX))
        {
            *model = Value::String(raw.to_string());
        }
    }
}

fn selected_model(message: &Value) -> Option<&str> {
    message.pointer("/params/model").and_then(Value::as_str)
}

fn is_grok_model(model: &str) -> bool {
    model.starts_with(BRIDGE_PREFIX)
}

fn thread_id(message: &Value) -> Option<&str> {
    message.pointer("/params/threadId").and_then(Value::as_str)
}

fn insert_default(object: &mut Map<String, Value>, key: &str, value: Value) {
    object.entry(key.to_string()).or_insert(value);
}

fn normalize_timestamp(object: &mut Map<String, Value>, key: &str, fallback: i64) {
    let normalized = object
        .get(key)
        .and_then(|value| {
            value.as_i64().or_else(|| {
                value.as_str().and_then(|text| {
                    time::OffsetDateTime::parse(
                        text,
                        &time::format_description::well_known::Rfc3339,
                    )
                    .ok()
                    .map(|date| date.unix_timestamp())
                })
            })
        })
        .unwrap_or(fallback);
    object.insert(key.to_string(), Value::Number(normalized.into()));
}

fn unix_now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn new_internal_id(next_id: &mut u64) -> String {
    let value = format!("{INTERNAL_ID_PREFIX}{next_id}");
    *next_id += 1;
    value
}

fn new_server_id(next_id: &mut u64) -> String {
    let value = format!("{SERVER_ID_PREFIX}{next_id}");
    *next_id += 1;
    value
}

fn params_cursor(message: &Value) -> Option<&str> {
    message.pointer("/params/cursor").and_then(Value::as_str)
}

fn encode_offset_cursor(kind: &str, offset: usize) -> String {
    let payload =
        serde_json::to_vec(&json!({ "kind": kind, "offset": offset })).expect("serializable");
    format!("bridge:{kind}:{}", URL_SAFE_NO_PAD.encode(payload))
}

fn decode_offset_cursor(cursor: Option<&str>, expected_kind: &str) -> Option<usize> {
    let cursor = cursor?;
    let encoded = cursor.strip_prefix(&format!("bridge:{expected_kind}:"))?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    let payload: Value = serde_json::from_slice(&bytes).ok()?;
    payload.get("offset")?.as_u64().map(|value| value as usize)
}

fn encode_thread_cursor(cursor: &CompositeThreadCursor) -> String {
    let payload = serde_json::to_vec(cursor).expect("serializable");
    format!("bridge:threads:{}", URL_SAFE_NO_PAD.encode(payload))
}

fn decode_thread_cursor(cursor: Option<&str>) -> Result<CompositeThreadCursor> {
    let Some(cursor) = cursor else {
        return Ok(CompositeThreadCursor::default());
    };
    let encoded = cursor
        .strip_prefix("bridge:threads:")
        .ok_or_else(|| anyhow!("invalid composite thread cursor"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .context("invalid composite thread cursor encoding")?;
    let cursor: CompositeThreadCursor =
        serde_json::from_slice(&bytes).context("invalid composite thread cursor payload")?;
    if cursor.version != 1 {
        bail!("unsupported composite thread cursor version");
    }
    Ok(cursor)
}

fn jsonrpc_error(id: Value, code: i64, message: String) -> Value {
    json!({ "id": id, "error": { "code": code, "message": message } })
}

async fn write_json(stdout: &mut tokio::io::Stdout, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    stdout.write_all(&bytes).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

async fn spawn_peer(
    backend: Backend,
    executable: &Path,
    args: &[OsString],
    cwd: &Path,
    events: mpsc::Sender<ChildEvent>,
    sanitize_native_env: bool,
) -> Result<ChildPeer> {
    let mut command = Command::new(executable);
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if sanitize_native_env {
        command.env_remove("CODEX_CLI_PATH");
        command.env_remove("CODEX_GROK_NATIVE_CODEX");
        command.env_remove("CODEX_GROK_GROK");
        command.env_remove("CODEX_APP_SERVER_FORCE_CLI");
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {backend:?} child {}", executable.display()))?;
    let stdin = child.stdin.take().context("child stdin unavailable")?;
    let stdout = child.stdout.take().context("child stdout unavailable")?;
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if events.send(ChildEvent::Line(backend, line)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    let _ = events.send(ChildEvent::Exited(backend, None)).await;
                    break;
                }
                Err(error) => {
                    warn!(?backend, %error, "child stdout failed");
                    let _ = events.send(ChildEvent::Exited(backend, None)).await;
                    break;
                }
            }
        }
    });
    Ok(ChildPeer { child, stdin })
}

pub async fn native_codex_version() -> Result<String> {
    command_version(&native_codex_path()).await
}

async fn verify_versions(native: &Path, grok: &Path) -> Result<()> {
    let native_version = command_version(native).await?;
    let grok_version = command_version(grok).await?;
    let skip = std::env::var_os("CODEX_GROK_SKIP_VERSION_CHECK").is_some();
    if !skip && !native_version.starts_with(SUPPORTED_CODEX_PREFIX) {
        bail!(
            "unsupported bundled Codex version {native_version:?}; expected {SUPPORTED_CODEX_PREFIX}x (set CODEX_GROK_SKIP_VERSION_CHECK=1 to test explicitly)"
        );
    }
    if !skip && !grok_version.starts_with(SUPPORTED_GROK_PREFIX) {
        bail!(
            "unsupported Grok Build version {grok_version:?}; expected {SUPPORTED_GROK_PREFIX}x (set CODEX_GROK_SKIP_VERSION_CHECK=1 to test explicitly)"
        );
    }
    info!(%native_version, %grok_version, "backend versions verified");
    Ok(())
}

async fn command_version(path: &Path) -> Result<String> {
    let output = Command::new(path)
        .arg("--version")
        .output()
        .await
        .with_context(|| format!("run {} --version", path.display()))?;
    if !output.status.success() {
        bail!("{} --version failed with {}", path.display(), output.status);
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn native_codex_path() -> PathBuf {
    std::env::var_os("CODEX_GROK_NATIVE_CODEX")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Applications/ChatGPT.app/Contents/Resources/codex"))
}

fn grok_path() -> PathBuf {
    std::env::var_os("CODEX_GROK_GROK")
        .map(PathBuf::from)
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_default().join(".grok/bin/grok"))
}

fn state_path() -> PathBuf {
    if let Some(path) = std::env::var_os("CODEX_GROK_STATE_PATH") {
        return PathBuf::from(path);
    }
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_default().join(".codex"));
    codex_home.join("grok-bridge/routes.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_is_namespaced_with_exact_capabilities() {
        let model = grok_model();
        assert_eq!(model["id"], "grok/grok-4.6");
        assert_eq!(model["inputModalities"], json!(["text"]));
        assert_eq!(
            model["supportedReasoningEfforts"].as_array().unwrap().len(),
            4
        );
    }

    #[test]
    fn cursor_round_trips() {
        let cursor = encode_offset_cursor("threads", 42);
        assert_eq!(decode_offset_cursor(Some(&cursor), "threads"), Some(42));
        assert_eq!(decode_offset_cursor(Some(&cursor), "models"), None);

        let composite = CompositeThreadCursor {
            native: BackendThreadCursor {
                cursor: Some("native-next".into()),
                offset: 7,
                exhausted: false,
            },
            grok: BackendThreadCursor {
                cursor: None,
                offset: 0,
                exhausted: true,
            },
            ..CompositeThreadCursor::default()
        };
        let encoded = encode_thread_cursor(&composite);
        assert_eq!(decode_thread_cursor(Some(&encoded)).unwrap(), composite);
    }

    #[test]
    fn server_request_ids_remain_unique_after_a_response_creates_a_hole() {
        let mut next_id = 1;
        let mut pending = HashMap::new();
        let first = new_server_id(&mut next_id);
        pending.insert(first.clone(), json!("approval-1"));
        let second = new_server_id(&mut next_id);
        pending.insert(second.clone(), json!("approval-2"));
        pending.remove(&first);
        let third = new_server_id(&mut next_id);
        pending.insert(third.clone(), json!("approval-3"));

        assert_ne!(second, third);
        assert_eq!(pending[&second], "approval-2");
        assert_eq!(pending[&third], "approval-3");
    }

    #[test]
    fn rejects_cross_backend_model_change() {
        let message = json!({"method":"turn/start","params":{"model":"gpt-5","threadId":"t"}});
        assert!(validate_model_boundary(&message, Backend::Grok).is_err());
        let message =
            json!({"method":"turn/start","params":{"model":"grok/grok-4.6","threadId":"t"}});
        assert!(validate_model_boundary(&message, Backend::Native).is_err());
    }

    #[test]
    fn sol_thread_creation_uses_the_vanilla_native_route() {
        let temporary = tempfile::tempdir().unwrap();
        let routes = RouteStore::load(temporary.path().join("routes.json")).unwrap();
        let message = json!({
            "id": 41,
            "method": "thread/start",
            "params": {"model": "gpt-5.6-sol"}
        });

        assert_eq!(route_request(&message, &routes).unwrap(), Backend::Native);
    }

    #[test]
    fn native_thread_creation_preserves_the_exact_client_line() {
        let original =
            r#"{ "id" : 41, "method" : "thread/start", "params" : { "model" : "gpt-5.6-sol" } }"#;

        assert_eq!(
            native_passthrough_line(Backend::Native, original),
            Some(original)
        );
        assert_eq!(native_passthrough_line(Backend::Grok, original), None);
    }

    #[test]
    fn native_thread_responses_never_create_bridge_routing_state() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("routes.json");
        let mut routes = RouteStore::load(path.clone()).unwrap();
        let response = json!({
            "id": 41,
            "result": {"thread": {"id": "native-thread"}}
        });

        record_route_from_response(&mut routes, Backend::Native, "thread/start", &response)
            .unwrap();

        assert_eq!(routes.backend("native-thread"), None);
        assert!(!path.exists());
    }

    #[test]
    fn normalizes_legacy_gateway_thread_shape() {
        let mut thread = json!({
            "id":"t",
            "createdAt":"2026-01-02T03:04:05Z",
            "updatedAt":"2026-01-02T03:04:06Z",
            "turns":[]
        });
        normalize_thread(&mut thread);
        assert!(thread["createdAt"].is_number());
        assert_eq!(thread["modelProvider"], "grok");
        assert_eq!(thread["sessionId"], "t");
        assert_eq!(thread["historyMode"], "full");
    }

    #[test]
    fn normalizes_grok_thread_response_identity() {
        let mut message = json!({
            "id": 1,
            "result": {
                "model": "grok-4.6",
                "modelProvider": "acp",
                "reasoningEffort": null,
                "thread": {"id": "t", "turns": []}
            }
        });
        normalize_grok_message(&mut message);
        assert_eq!(message["result"]["model"], "grok/grok-4.6");
        assert_eq!(message["result"]["modelProvider"], "grok");
        assert_eq!(message["result"]["reasoningEffort"], "high");
    }

    #[test]
    fn normalizes_command_approval_for_current_app_server() {
        let mut message = json!({
            "id": "grok:approval-1",
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "t", "turnId": "turn", "itemId": "item"}
        });
        normalize_grok_message(&mut message);
        let params = &message["params"];
        assert!(params["startedAtMs"].is_number());
        assert_eq!(
            params["availableDecisions"],
            json!(["accept", "acceptForSession", "decline", "cancel"])
        );
    }

    #[test]
    fn persists_backend_ownership_across_restarts() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("routes.json");
        let mut routes = RouteStore::load(path.clone()).unwrap();
        routes
            .insert(
                "thread-1".into(),
                RouteRecord {
                    backend: Backend::Grok,
                    model: "grok/grok-4.6".into(),
                    acp_session_id: Some("session-1".into()),
                },
            )
            .unwrap();

        let reloaded = RouteStore::load(path).unwrap();
        assert_eq!(reloaded.backend("thread-1"), Some(Backend::Grok));
        assert_eq!(
            reloaded.file.threads["thread-1"].acp_session_id.as_deref(),
            Some("session-1")
        );
    }

    #[test]
    fn thread_list_preserves_bridge_public_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let mut routes = RouteStore::load(temporary.path().join("routes.json")).unwrap();
        routes
            .insert(
                "thread-1".into(),
                RouteRecord {
                    backend: Backend::Grok,
                    model: "grok/grok-4.6".into(),
                    acp_session_id: Some("session-1".into()),
                },
            )
            .unwrap();
        let result = merge_threads(
            &json!({"result":{"data":[]}}),
            &json!({"result":{"data":[{"id":"session-1","turns":[]}]}}),
            &json!({"limit":20}),
            &mut routes,
        )
        .unwrap();
        assert_eq!(result["data"][0]["id"], "thread-1");
        assert_eq!(result["data"][0]["sessionId"], "thread-1");
    }

    #[test]
    fn thread_pagination_retains_each_backend_cursor_and_page_offset() {
        let temporary = tempfile::tempdir().unwrap();
        let mut routes = RouteStore::load(temporary.path().join("routes.json")).unwrap();
        let native = json!({
            "result": {
                "data": [
                    {"id":"native-4","createdAt":4},
                    {"id":"native-2","createdAt":2}
                ],
                "nextCursor":"native-next"
            }
        });
        let grok = json!({
            "result": {
                "data": [
                    {"id":"grok-3","createdAt":3,"turns":[]},
                    {"id":"grok-1","createdAt":1,"turns":[]}
                ],
                "nextCursor":"grok-next"
            }
        });

        let first = merge_threads(
            &native,
            &grok,
            &json!({"limit":3,"sortKey":"created_at","sortDirection":"desc"}),
            &mut routes,
        )
        .unwrap();
        let ids: Vec<&str> = first["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|thread| thread["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["native-4", "grok-3", "native-2"]);

        let cursor = decode_thread_cursor(first["nextCursor"].as_str()).unwrap();
        assert_eq!(cursor.native.cursor.as_deref(), Some("native-next"));
        assert_eq!(cursor.native.offset, 0);
        assert_eq!(cursor.grok.cursor, None);
        assert_eq!(cursor.grok.offset, 1);
        assert_eq!(cursor.history.len(), 1);
        assert_eq!(cursor.history[0].native, BackendThreadCursor::default());
        assert_eq!(cursor.history[0].grok, BackendThreadCursor::default());
    }
}
