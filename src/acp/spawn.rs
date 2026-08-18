use std::path::Path;
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

/// Spawn an ACP agent as a subprocess with stdin/stdout piped for JSON-RPC
/// communication and stderr inherited so the agent's diagnostics appear in our
/// terminal.
pub fn spawn_subprocess(cmd: &str, args: &[String], cwd: &Path) -> Result<Child> {
    Command::new(cmd)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to spawn ACP agent: {cmd}"))
}
