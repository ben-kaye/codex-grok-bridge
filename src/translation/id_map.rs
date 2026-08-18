use std::collections::HashMap;
use std::path::{Path, PathBuf};

use agent_client_protocol::ToolKind;
use serde_json::Value;
use uuid::Uuid;

/// Bidirectional ID mapping between Codex and ACP identifiers.
///
/// Codex uses thread IDs, turn IDs, and item IDs while ACP uses session IDs
/// and tool call IDs. This struct maintains the mapping in both directions.
#[derive(Debug, Default)]
pub struct IdMap {
    /// Codex threadId -> ACP sessionId
    thread_to_session: HashMap<String, String>,
    /// ACP sessionId -> Codex threadId
    session_to_thread: HashMap<String, String>,
    /// ACP toolCallId -> Codex itemId
    tool_to_item: HashMap<String, String>,
    /// Codex itemId -> ACP toolCallId
    item_to_tool: HashMap<String, String>,
    /// ACP toolCallId -> ToolKind (for correct item/completed type)
    tool_kind: HashMap<String, ToolKind>,
    /// ACP toolCallId -> original title (completion updates may omit it).
    tool_title: HashMap<String, String>,
    /// Latest Codex-facing file changes for an ACP tool call.
    tool_file_changes: HashMap<String, Vec<Value>>,
    /// Tool calls already projected to the UI as native file-change items.
    file_change_items_started: std::collections::HashSet<String>,
    /// Location of the durable thread-to-session map. Tool and turn IDs are
    /// deliberately process-local.
    persistence_path: Option<PathBuf>,
}

impl IdMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load durable Codex thread -> ACP session ownership.
    pub fn load(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let thread_to_session: HashMap<String, String> = std::fs::read_to_string(&path)
            .ok()
            .and_then(|contents| serde_json::from_str(&contents).ok())
            .unwrap_or_default();
        let session_to_thread = thread_to_session
            .iter()
            .map(|(thread, session)| (session.clone(), thread.clone()))
            .collect();
        Self {
            thread_to_session,
            session_to_thread,
            persistence_path: Some(path),
            ..Self::default()
        }
    }

    /// Create a bidirectional mapping between a Codex thread ID and an ACP session ID.
    pub fn create_thread_session_mapping(&mut self, thread_id: String, session_id: String) {
        self.thread_to_session
            .insert(thread_id.clone(), session_id.clone());
        self.session_to_thread.insert(session_id, thread_id);
        self.persist();
    }

    fn persist(&self) {
        let Some(path) = &self.persistence_path else {
            return;
        };
        let Some(parent) = path.parent() else { return };
        if let Err(error) = std::fs::create_dir_all(parent) {
            tracing::warn!(%error, path = %path.display(), "failed to create ACP route directory");
            return;
        }
        let temporary = path.with_extension("json.tmp");
        let result = serde_json::to_vec_pretty(&self.thread_to_session)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(&temporary, bytes))
            .and_then(|_| std::fs::rename(&temporary, path));
        if let Err(error) = result {
            tracing::warn!(%error, path = %path.display(), "failed to persist ACP route map");
        }
    }

    /// Look up the ACP session ID for a Codex thread ID.
    pub fn lookup_session(&self, thread_id: &str) -> Option<&String> {
        self.thread_to_session.get(thread_id)
    }

    /// Look up the Codex thread ID for an ACP session ID.
    pub fn lookup_thread(&self, session_id: &str) -> Option<&String> {
        self.session_to_thread.get(session_id)
    }

    /// Create a new Codex item ID for an ACP tool call ID and store the mapping.
    /// Returns the newly generated item ID.
    pub fn create_item_for_tool(&mut self, tool_call_id: &str) -> String {
        if let Some(item_id) = self.tool_to_item.get(tool_call_id) {
            return item_id.clone();
        }
        let item_id = new_item_id();
        self.tool_to_item
            .insert(tool_call_id.to_string(), item_id.clone());
        self.item_to_tool
            .insert(item_id.clone(), tool_call_id.to_string());
        item_id
    }

    /// Look up the Codex item ID for an ACP tool call ID.
    pub fn lookup_item(&self, tool_call_id: &str) -> Option<&String> {
        self.tool_to_item.get(tool_call_id)
    }

    /// Look up the ACP tool call ID for a Codex item ID.
    pub fn lookup_tool(&self, item_id: &str) -> Option<&String> {
        self.item_to_tool.get(item_id)
    }

    /// Store the ACP ToolKind for a tool call ID.
    pub fn set_tool_kind(&mut self, tool_call_id: &str, kind: ToolKind) {
        self.tool_kind.insert(tool_call_id.to_string(), kind);
    }

    /// Look up the ToolKind for an ACP tool call ID.
    pub fn lookup_tool_kind(&self, tool_call_id: &str) -> Option<&ToolKind> {
        self.tool_kind.get(tool_call_id)
    }

    /// Preserve display metadata from the initial tool call.
    pub fn set_tool_title(&mut self, tool_call_id: &str, title: String) {
        self.tool_title.insert(tool_call_id.to_string(), title);
    }

    pub fn lookup_tool_title(&self, tool_call_id: &str) -> Option<&str> {
        self.tool_title.get(tool_call_id).map(String::as_str)
    }

    pub fn set_tool_file_changes(&mut self, tool_call_id: &str, changes: Vec<Value>) {
        self.tool_file_changes
            .insert(tool_call_id.to_string(), changes);
    }

    pub fn lookup_tool_file_changes(&self, tool_call_id: &str) -> Option<&[Value]> {
        self.tool_file_changes.get(tool_call_id).map(Vec::as_slice)
    }

    pub fn mark_file_change_item_started(&mut self, tool_call_id: &str) {
        self.file_change_items_started
            .insert(tool_call_id.to_string());
    }

    pub fn file_change_item_started(&self, tool_call_id: &str) -> bool {
        self.file_change_items_started.contains(tool_call_id)
    }
}

/// Generate a new Codex thread ID using UUID v7 (time-ordered).
pub fn new_thread_id() -> String {
    Uuid::now_v7().to_string()
}

/// Generate a new Codex turn ID using UUID v7.
pub fn new_turn_id() -> String {
    Uuid::now_v7().to_string()
}

/// Generate a new ACP prompt ID using UUID v7.
pub fn new_prompt_id() -> String {
    Uuid::now_v7().to_string()
}

/// Generate a new Codex item ID using UUID v7.
pub fn new_item_id() -> String {
    Uuid::now_v7().to_string()
}
