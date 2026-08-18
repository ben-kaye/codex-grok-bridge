//! Simplified rollout recorder for the gateway.
//!
//! Writes rollout files in the same NDJSON format as Codex
//! (`RolloutLine { timestamp, RolloutItem }`), but uses synchronous I/O
//! instead of Codex's background writer task.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use codex_protocol::protocol::{RolloutItem, RolloutLine, SessionMetaLine};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::warn;

use super::SESSIONS_SUBDIR;

/// Synchronous rollout file recorder.
///
/// Each thread gets a `.jsonl` file under
/// `<gateway_home>/sessions/YYYY/MM/DD/rollout-<timestamp>-<uuid>.jsonl`.
/// The first line is a `RolloutItem::SessionMeta(SessionMetaLine)`;
/// subsequent lines are `RolloutItem::EventMsg(...)` or other variants.
pub struct RolloutRecorder {
    /// Root directory for gateway data (e.g. `~/.codex-acp-gateway/`).
    gateway_home: PathBuf,
    /// Open file handles keyed by thread ID string.
    open_files: HashMap<String, std::io::BufWriter<std::fs::File>>,
    /// Quick lookup from thread ID to its `.jsonl` path.
    thread_files: HashMap<String, PathBuf>,
}

impl std::fmt::Debug for RolloutRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RolloutRecorder")
            .field("gateway_home", &self.gateway_home)
            .field("thread_files", &self.thread_files)
            .finish()
    }
}

impl RolloutRecorder {
    /// Create a new recorder. `gateway_home` defaults to `~/.codex-acp-gateway/`.
    pub fn new(gateway_home: Option<PathBuf>) -> Self {
        let gateway_home = gateway_home.unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".codex-acp-gateway")
        });
        Self {
            gateway_home,
            open_files: HashMap::new(),
            thread_files: HashMap::new(),
        }
    }

    /// Return the gateway home directory.
    pub fn gateway_home(&self) -> &Path {
        &self.gateway_home
    }

    /// Return the sessions directory (`<gateway_home>/sessions/`).
    pub fn sessions_dir(&self) -> PathBuf {
        self.gateway_home.join(SESSIONS_SUBDIR)
    }

    /// Start recording a new thread. Creates the date-partitioned directory and
    /// writes the `SessionMetaLine` as the first rollout line.
    ///
    /// Returns the path to the newly created rollout file.
    pub fn start_thread(&mut self, meta: &SessionMetaLine) -> Option<PathBuf> {
        let now = OffsetDateTime::now_utc();
        let thread_id = meta.meta.id.to_string();

        // Build date path: sessions/YYYY/MM/DD/
        let year = now.year();
        let month = now.month() as u8;
        let day = now.day();
        let dir = self
            .gateway_home
            .join(SESSIONS_SUBDIR)
            .join(format!("{year:04}"))
            .join(format!("{month:02}"))
            .join(format!("{day:02}"));

        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("failed to create sessions dir {}: {e}", dir.display());
            return None;
        }

        // Filename: rollout-YYYY-MM-DDThh-mm-ss-<uuid>.jsonl
        let hour = now.hour();
        let minute = now.minute();
        let second = now.second();
        let file_name = format!(
            "rollout-{year:04}-{month:02}-{day:02}T{hour:02}-{minute:02}-{second:02}-{}.jsonl",
            meta.meta.id
        );
        let file_path = dir.join(&file_name);

        let file = match std::fs::File::create(&file_path) {
            Ok(f) => f,
            Err(e) => {
                warn!("failed to create rollout file {}: {e}", file_path.display());
                return None;
            }
        };

        let mut writer = std::io::BufWriter::new(file);

        // Write the SessionMeta header as the first RolloutLine
        let line = RolloutLine {
            timestamp: now_rfc3339(),
            item: RolloutItem::SessionMeta(meta.clone()),
        };
        if let Err(e) = write_rollout_line(&mut writer, &line) {
            warn!("failed to write rollout header: {e}");
            return None;
        }

        self.thread_files
            .insert(thread_id.clone(), file_path.clone());
        self.open_files.insert(thread_id, writer);

        Some(file_path)
    }

    /// Append a `RolloutItem` to the rollout file for the given thread.
    pub fn record_item(&mut self, thread_id: &str, item: RolloutItem) {
        let writer = match self.open_files.get_mut(thread_id) {
            Some(w) => w,
            None => return, // Thread not being recorded
        };
        let line = RolloutLine {
            timestamp: now_rfc3339(),
            item,
        };
        if let Err(e) = write_rollout_line(writer, &line) {
            warn!(%thread_id, "failed to write rollout item: {e}");
        }
    }

    /// Find the `.jsonl` file for a given thread ID.
    ///
    /// First checks the in-memory map, then falls back to scanning the
    /// sessions directory.
    pub fn find_thread_file(&self, thread_id: &str) -> Option<PathBuf> {
        if let Some(p) = self.thread_files.get(thread_id) {
            return Some(p.clone());
        }
        // Fall back to scanning (handles threads from previous sessions)
        scan_for_thread_file(&self.sessions_dir(), thread_id)
    }

    /// Flush and close the writer for a thread.
    pub fn close_file(&mut self, thread_id: &str) {
        if let Some(mut writer) = self.open_files.remove(thread_id) {
            let _ = writer.flush();
        }
    }

    /// Load all `RolloutItem` entries from a rollout file.
    ///
    /// Parses each line as a `RolloutLine` and returns the items in order.
    pub fn load_rollout_items(path: &Path) -> Vec<RolloutItem> {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                warn!("failed to read rollout file {}: {e}", path.display());
                return Vec::new();
            }
        };
        let mut items = Vec::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<RolloutLine>(trimmed) {
                Ok(rl) => items.push(rl.item),
                Err(e) => {
                    warn!("failed to parse rollout line: {e}");
                }
            }
        }
        items
    }
}

impl Default for RolloutRecorder {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Scan `sessions_dir` subdirectories for a rollout file containing
/// the given thread ID in its filename.
fn scan_for_thread_file(sessions_dir: &Path, thread_id: &str) -> Option<PathBuf> {
    // Walk YYYY/MM/DD directories looking for rollout-...-<uuid>.jsonl
    let years = std::fs::read_dir(sessions_dir).ok()?;
    for year_entry in years.flatten() {
        if !year_entry.file_type().ok()?.is_dir() {
            continue;
        }
        let months = match std::fs::read_dir(year_entry.path()) {
            Ok(m) => m,
            Err(_) => continue,
        };
        for month_entry in months.flatten() {
            if !month_entry.file_type().ok()?.is_dir() {
                continue;
            }
            let days = match std::fs::read_dir(month_entry.path()) {
                Ok(d) => d,
                Err(_) => continue,
            };
            for day_entry in days.flatten() {
                if !day_entry.file_type().ok()?.is_dir() {
                    continue;
                }
                let files = match std::fs::read_dir(day_entry.path()) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                for file_entry in files.flatten() {
                    let path = file_entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str())
                        && name.ends_with(".jsonl")
                        && name.contains(thread_id)
                    {
                        return Some(path);
                    }
                }
            }
        }
    }
    None
}

/// Write a `RolloutLine` as a single JSON line followed by a newline.
fn write_rollout_line(
    writer: &mut std::io::BufWriter<std::fs::File>,
    line: &RolloutLine,
) -> std::io::Result<()> {
    let json = serde_json::to_string(line).map_err(std::io::Error::other)?;
    writeln!(writer, "{json}")?;
    writer.flush()
}

/// Get the current UTC time as an RFC 3339 string.
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}
