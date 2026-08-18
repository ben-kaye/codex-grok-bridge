//! Rollout module: persistence and discovery of session rollout files.
//!
//! Vendored from `codex-core/src/rollout/` with simplifications:
//! - No SQLite state DB (filesystem is the only source of truth)
//! - Synchronous recorder (no background writer task)
//! - No `codex-core` Config dependency

use codex_protocol::protocol::SessionSource;

pub const SESSIONS_SUBDIR: &str = "sessions";
pub const ARCHIVED_SESSIONS_SUBDIR: &str = "archived_sessions";
pub const INTERACTIVE_SESSION_SOURCES: &[SessionSource] =
    &[SessionSource::Cli, SessionSource::VSCode];

pub mod list;
pub mod recorder;
pub mod session_index;
