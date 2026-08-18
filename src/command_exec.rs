//! Shared `command/exec` handler used by both Codex and ACP-proxy modes.
//!
//! Executes a subprocess with an explicit argument list (no shell invocation)
//! and returns structured `{exitCode, stdout, stderr}` output.  When a
//! `sandboxPolicy` is provided, the command is wrapped inside bubblewrap
//! (`bwrap`) for filesystem and network isolation.

use std::path::Path;

use anyhow::{Result, bail};
use serde_json::{Value, json};
use tracing::debug;

use crate::sandbox;

/// Execute a command described by JSON-RPC `params` and return the result.
///
/// Expected params shape:
/// ```json
/// {
///   "command": ["ls", "-la"],
///   "cwd": "/optional/override",
///   "timeoutMs": 30000,
///   "sandboxPolicy": { "type": "workspaceWrite" }
/// }
/// ```
pub async fn run(params: &Value, default_cwd: &Path) -> Result<Value> {
    let command: Vec<String> = params
        .get("command")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    if command.is_empty() {
        bail!("command/exec: missing or empty 'command' array");
    }

    let timeout_ms = params
        .get("timeoutMs")
        .and_then(|v| v.as_i64())
        .unwrap_or(30_000);

    let exec_cwd = params
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| default_cwd.to_path_buf());

    // Parse optional sandbox policy (v2 wire format: camelCase tags).
    // Fail closed: if the field is present but malformed, reject the request
    // rather than silently running unsandboxed.
    let sandbox_policy = match params.get("sandboxPolicy") {
        Some(v) if !v.is_null() => {
            let v2: codex_app_server_protocol::SandboxPolicy = serde_json::from_value(v.clone())
                .map_err(|e| anyhow::anyhow!("command/exec: invalid sandboxPolicy: {e}"))?;
            Some(v2.to_core())
        }
        _ => None,
    };

    // Wrap the command with bwrap if a restrictive policy is set.
    let (program, args) = if let Some(ref policy) = sandbox_policy {
        debug!(policy = ?policy, "command/exec: applying sandbox policy");
        sandbox::wrap_command(&command[0], &command[1..], &exec_cwd, policy)?
    } else {
        (command[0].clone(), command[1..].to_vec())
    };

    debug!(cmd = %program, args = ?args, cwd = %exec_cwd.display(), "command/exec");

    let child = tokio::process::Command::new(&program)
        .args(&args)
        .current_dir(&exec_cwd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let output = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms as u64),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("command/exec timed out after {timeout_ms}ms"))??;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let exit_code = output.status.code().unwrap_or(-1);

    Ok(json!({
        "exitCode": exit_code,
        "stdout": stdout,
        "stderr": stderr,
    }))
}
