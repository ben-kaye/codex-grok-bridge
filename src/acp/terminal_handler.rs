use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use agent_client_protocol::TerminalExitStatus;
use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify};

/// Shared output buffer written by the background reader task and read by the
/// terminal manager on demand.
struct OutputBuffer {
    data: Vec<u8>,
    byte_limit: Option<u64>,
}

impl OutputBuffer {
    fn append(&mut self, chunk: &[u8]) {
        self.data.extend_from_slice(chunk);
        self.enforce_limit();
    }

    fn enforce_limit(&mut self) {
        if let Some(limit) = self.byte_limit {
            let limit = limit as usize;
            if self.data.len() > limit {
                let excess = self.data.len() - limit;
                // Find a valid UTF-8 char boundary at or after `excess`
                let drain_to = match std::str::from_utf8(&self.data) {
                    Ok(s) => {
                        let mut boundary = excess;
                        while !s.is_char_boundary(boundary) && boundary < s.len() {
                            boundary += 1;
                        }
                        boundary
                    }
                    Err(_) => excess,
                };
                self.data.drain(..drain_to);
            }
        }
    }
}

/// State for a single managed terminal process.
struct ManagedTerminal {
    child: Child,
    output: Arc<Mutex<OutputBuffer>>,
    exit_status: Arc<Mutex<Option<TerminalExitStatus>>>,
    exit_notify: Arc<Notify>,
}

/// Manages spawned terminal processes on behalf of the ACP agent.
pub struct TerminalManager {
    terminals: HashMap<String, ManagedTerminal>,
    next_id: u64,
}

impl Default for TerminalManager {
    fn default() -> Self {
        Self {
            terminals: HashMap::new(),
            next_id: 1,
        }
    }
}

impl TerminalManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a new terminal process and return its ID.
    pub async fn create(
        &mut self,
        command: &str,
        args: &[String],
        cwd: Option<&PathBuf>,
        env: &[(String, String)],
        output_byte_limit: Option<u64>,
    ) -> Result<String> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }

        for (key, val) in env {
            cmd.env(key, val);
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn terminal command: {command}"))?;

        let id = self.next_id;
        self.next_id += 1;
        let terminal_id = format!("term-{id}");

        let output = Arc::new(Mutex::new(OutputBuffer {
            data: Vec::new(),
            byte_limit: output_byte_limit,
        }));
        let exit_status: Arc<Mutex<Option<TerminalExitStatus>>> = Arc::new(Mutex::new(None));
        let exit_notify = Arc::new(Notify::new());

        // Take stdout and stderr from the child and spawn background readers.
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        if let Some(mut stdout) = stdout {
            let buf = Arc::clone(&output);
            tokio::task::spawn_local(async move {
                let mut tmp = [0u8; 8192];
                loop {
                    match stdout.read(&mut tmp).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.lock().await.append(&tmp[..n]),
                    }
                }
            });
        }

        if let Some(mut stderr) = stderr {
            let buf = Arc::clone(&output);
            tokio::task::spawn_local(async move {
                let mut tmp = [0u8; 8192];
                loop {
                    match stderr.read(&mut tmp).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.lock().await.append(&tmp[..n]),
                    }
                }
            });
        }

        self.terminals.insert(
            terminal_id.clone(),
            ManagedTerminal {
                child,
                output,
                exit_status,
                exit_notify,
            },
        );

        Ok(terminal_id)
    }

    /// Get current output and exit status for a terminal.
    pub async fn output(
        &mut self,
        terminal_id: &str,
    ) -> Result<(String, bool, Option<TerminalExitStatus>)> {
        // Check if child has exited.
        self.check_exit(terminal_id).await;

        let term = self
            .terminals
            .get(terminal_id)
            .with_context(|| format!("unknown terminal: {terminal_id}"))?;

        let buf = term.output.lock().await;
        let truncated = buf
            .byte_limit
            .is_some_and(|limit| buf.data.len() >= limit as usize);
        let text = String::from_utf8_lossy(&buf.data).into_owned();
        let status = term.exit_status.lock().await.clone();

        Ok((text, truncated, status))
    }

    /// Check if a terminal's child has exited and record the status.
    async fn check_exit(&mut self, terminal_id: &str) {
        if let Some(term) = self.terminals.get_mut(terminal_id) {
            if term.exit_status.lock().await.is_some() {
                return;
            }
            if let Ok(Some(status)) = term.child.try_wait() {
                let exit_status =
                    TerminalExitStatus::new().exit_code(status.code().map(|c| c as u32));
                *term.exit_status.lock().await = Some(exit_status);
                term.exit_notify.notify_waiters();
            }
        }
    }

    /// Wait for the terminal process to exit and return its status.
    pub async fn wait_for_exit(&mut self, terminal_id: &str) -> Result<TerminalExitStatus> {
        // First check if already exited.
        self.check_exit(terminal_id).await;

        let term = self
            .terminals
            .get(terminal_id)
            .with_context(|| format!("unknown terminal: {terminal_id}"))?;

        if let Some(status) = term.exit_status.lock().await.clone() {
            return Ok(status);
        }

        let exit_status = Arc::clone(&term.exit_status);
        let exit_notify = Arc::clone(&term.exit_notify);

        // Poll until the child exits.
        loop {
            self.check_exit(terminal_id).await;
            if let Some(status) = exit_status.lock().await.clone() {
                return Ok(status);
            }
            // Wait a bit and check again, or wait for notification.
            tokio::select! {
                _ = exit_notify.notified() => {
                    if let Some(status) = exit_status.lock().await.clone() {
                        return Ok(status);
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }

    /// Kill the terminal process without releasing the terminal ID.
    pub async fn kill(&mut self, terminal_id: &str) -> Result<()> {
        let term = self
            .terminals
            .get_mut(terminal_id)
            .with_context(|| format!("unknown terminal: {terminal_id}"))?;
        if term.exit_status.lock().await.is_none() {
            let _ = term.child.kill().await;
        }
        self.check_exit(terminal_id).await;
        Ok(())
    }

    /// Kill the process (if running) and remove the terminal from the map.
    pub async fn release(&mut self, terminal_id: &str) -> Result<()> {
        if let Some(mut term) = self.terminals.remove(terminal_id)
            && term.exit_status.lock().await.is_none()
        {
            let _ = term.child.kill().await;
        }
        Ok(())
    }
}
