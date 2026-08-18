use std::collections::HashMap;

use agent_client_protocol::{
    AgentCapabilities, AuthMethod, SessionConfigOption, SessionModeState, SessionModelState,
};
use serde_json::Value;

use crate::rollout::recorder::RolloutRecorder;

/// The latest ACP diff content for one tool call. Replacing a set with the
/// same tool-call ID preserves ACP collection-update semantics and prevents a
/// completion update from duplicating a previously streamed diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDiffSet {
    pub tool_call_id: String,
    pub diffs: Vec<String>,
}

/// Tracks the lifecycle state of a single Codex thread / ACP session pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionLifecycle {
    /// The thread has not yet been mapped to an ACP session.
    Uninitialized,
    /// A `session/new` request is in-flight.
    Creating,
    /// A `session/load` request is in-flight.
    Loading,
    /// The ACP session exists but no turn is active.
    Idle { session_id: String },
    /// A prompt turn is in progress.
    InTurn { session_id: String, turn_id: String },
    /// The session has been closed or the thread archived.
    Closed,
}

/// Content accumulated from incremental notifications for one active turn.
///
/// Codex treats `item/completed` as authoritative, so the completed item must
/// contain the text that was previously delivered as deltas.
#[derive(Debug, PartialEq, Eq)]
pub struct AgentMessageOutput {
    pub item_id: String,
    pub text: String,
}

#[derive(Debug)]
pub struct TurnOutput {
    active_agent_message: Option<AgentMessageOutput>,
    next_agent_message_index: usize,
    pub reasoning_summary: String,
    pub started_at_ms: i64,
}

impl TurnOutput {
    pub fn new(started_at_ms: i64) -> Self {
        Self {
            active_agent_message: None,
            next_agent_message_index: 0,
            reasoning_summary: String::new(),
            started_at_ms,
        }
    }

    /// Return the active message item, creating a new ordered segment when
    /// assistant text resumes after tool activity.
    pub fn ensure_agent_message(&mut self, turn_id: &str) -> (&AgentMessageOutput, bool) {
        let was_started = self.active_agent_message.is_none();
        if was_started {
            let item_id = format!("{turn_id}-msg-{}", self.next_agent_message_index);
            self.next_agent_message_index += 1;
            self.active_agent_message = Some(AgentMessageOutput {
                item_id,
                text: String::new(),
            });
        }
        (self.active_agent_message.as_ref().unwrap(), was_started)
    }

    pub fn take_agent_message(&mut self) -> Option<AgentMessageOutput> {
        self.active_agent_message.take()
    }

    pub fn apply_notification(&mut self, method: &str, params: &Value) {
        let Some(delta) = params.get("delta").and_then(Value::as_str) else {
            return;
        };
        match method {
            "item/agentMessage/delta" => {
                if let Some(message) = self.active_agent_message.as_mut() {
                    message.text.push_str(delta);
                }
            }
            "item/reasoning/summaryTextDelta" => self.reasoning_summary.push_str(delta),
            _ => {}
        }
    }
}

/// Shared gateway state tracking initialization and per-thread lifecycles.
#[derive(Debug)]
pub struct GatewayState {
    /// Whether the Codex `initialize` handshake has completed.
    pub initialized: bool,
    /// Client info extracted from the Codex `initialize` request.
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    /// Per-thread lifecycle state keyed by Codex thread ID.
    pub threads: HashMap<String, SessionLifecycle>,
    /// Agent capabilities from the ACP initialize response.
    pub agent_capabilities: Option<AgentCapabilities>,
    /// Auth methods advertised by the ACP agent.
    pub auth_methods: Vec<AuthMethod>,
    /// Per-session mode state (keyed by ACP session ID).
    pub session_modes: HashMap<String, SessionModeState>,
    /// Per-session config options (keyed by ACP session ID).
    pub session_config: HashMap<String, Vec<SessionConfigOption>>,
    /// Latest file diff collection for each tool call in a turn. Entries retain
    /// first-seen tool ordering while repeated ACP updates replace their
    /// collection instead of being appended.
    pub turn_diffs: HashMap<String, Vec<ToolDiffSet>>,
    /// Streamed assistant and reasoning text per active turn.
    pub turn_outputs: HashMap<String, TurnOutput>,
    /// Follow-up input queued while the current prompt cancellation completes.
    /// Remove this when the planned Grok send-now prompt path lands end to end.
    pub pending_steers: HashMap<String, Value>,
    /// Newest ACP prompt ID for each active Codex turn. A send-now steer
    /// replaces this so a cancelled predecessor cannot complete the turn.
    pub active_prompt_ids: HashMap<String, String>,
    /// Agent's model selection state, populated from session creation/load
    /// responses when the agent advertises `unstable_session_model` support.
    pub model_state: Option<SessionModelState>,
    /// Rollout recorder for persisting thread history to disk.
    /// Uses Codex-compatible `RolloutLine`/`RolloutItem` NDJSON format.
    pub rollout: RolloutRecorder,
}

impl GatewayState {
    pub fn new() -> Self {
        Self {
            initialized: false,
            client_name: None,
            client_version: None,
            threads: HashMap::new(),
            agent_capabilities: None,
            auth_methods: Vec::new(),
            session_modes: HashMap::new(),
            session_config: HashMap::new(),
            turn_diffs: HashMap::new(),
            turn_outputs: HashMap::new(),
            pending_steers: HashMap::new(),
            active_prompt_ids: HashMap::new(),
            model_state: None,
            rollout: RolloutRecorder::default(),
        }
    }

    /// Get the lifecycle for a thread, defaulting to `Uninitialized`.
    pub fn lifecycle(&self, thread_id: &str) -> &SessionLifecycle {
        self.threads
            .get(thread_id)
            .unwrap_or(&SessionLifecycle::Uninitialized)
    }

    /// Set the lifecycle for a thread.
    pub fn set_lifecycle(&mut self, thread_id: String, state: SessionLifecycle) {
        self.threads.insert(thread_id, state);
    }
}

impl Default for GatewayState {
    fn default() -> Self {
        Self::new()
    }
}
