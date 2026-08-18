use std::rc::Rc;

use agent_client_protocol::{
    Client, CreateTerminalRequest, CreateTerminalResponse, ExtNotification, ExtRequest,
    ExtResponse, KillTerminalCommandRequest, KillTerminalCommandResponse, ReadTextFileRequest,
    ReadTextFileResponse, ReleaseTerminalRequest, ReleaseTerminalResponse,
    RequestPermissionRequest, RequestPermissionResponse, Result as AcpResult, SessionNotification,
    TerminalOutputRequest, TerminalOutputResponse, WaitForTerminalExitRequest,
    WaitForTerminalExitResponse, WriteTextFileRequest, WriteTextFileResponse,
};
use tokio::sync::Mutex;

use super::fs_handler;
use super::terminal_handler::TerminalManager;

/// Events that our ACP client implementation forwards to the gateway's
/// translation / orchestration layer.
#[derive(Debug)]
pub enum AcpEvent {
    /// A session update notification from the agent.
    SessionNotification(SessionNotification),
    /// The agent is requesting permission for a tool call.
    PermissionRequest {
        request: RequestPermissionRequest,
        /// Send the response back through this channel.
        response_tx: tokio::sync::oneshot::Sender<RequestPermissionResponse>,
    },
    /// The agent is calling an extension method (e.g. `tool/call`,
    /// `request_user_input`).  The gateway translates these into Codex
    /// server requests and forwards to the Codex client.
    ExtMethodRequest {
        request: ExtRequest,
        /// Send the response (or error) back through this channel.
        response_tx: tokio::sync::oneshot::Sender<AcpResult<ExtResponse>>,
    },
}

/// Our implementation of the ACP `Client` trait.
///
/// Constructed once per ACP connection. The SDK calls into this whenever the
/// agent sends a request or notification *to us* (the client side).
///
/// Because the ACP SDK uses `!Send` futures (`async_trait(?Send)`), this struct
/// does not need to be `Send`.
pub struct GatewayAcpClient {
    /// Channel for forwarding events to the gateway event loop.
    event_tx: tokio::sync::mpsc::UnboundedSender<AcpEvent>,
    /// Terminal process manager -- behind a tokio Mutex so we can hold the
    /// guard across `.await` points within the `Client` trait methods.
    terminal_manager: Rc<Mutex<TerminalManager>>,
}

impl std::fmt::Debug for GatewayAcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayAcpClient").finish_non_exhaustive()
    }
}

impl GatewayAcpClient {
    pub fn new(event_tx: tokio::sync::mpsc::UnboundedSender<AcpEvent>) -> Self {
        Self {
            event_tx,
            terminal_manager: Rc::new(Mutex::new(TerminalManager::new())),
        }
    }
}

/// Map an `anyhow::Error` into an ACP protocol error.
fn internal_err(e: anyhow::Error) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(format!("{e:#}"))
}

#[async_trait::async_trait(?Send)]
impl Client for GatewayAcpClient {
    async fn session_notification(&self, args: SessionNotification) -> AcpResult<()> {
        tracing::debug!(?args.update, "session notification from agent");
        let _ = self.event_tx.send(AcpEvent::SessionNotification(args));
        Ok(())
    }

    async fn request_permission(
        &self,
        args: RequestPermissionRequest,
    ) -> AcpResult<RequestPermissionResponse> {
        tracing::info!(session_id = %args.session_id, "agent requesting permission");
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.event_tx
            .send(AcpEvent::PermissionRequest {
                request: args,
                response_tx,
            })
            .map_err(|_| {
                agent_client_protocol::Error::internal_error().data("gateway event loop shut down")
            })?;
        response_rx.await.map_err(|_| {
            agent_client_protocol::Error::internal_error()
                .data("permission response channel dropped")
        })
    }

    async fn read_text_file(&self, args: ReadTextFileRequest) -> AcpResult<ReadTextFileResponse> {
        tracing::debug!(path = %args.path.display(), "agent reading file");
        let content = fs_handler::read_text_file(&args.path, args.line, args.limit)
            .await
            .map_err(internal_err)?;
        Ok(ReadTextFileResponse::new(content))
    }

    async fn write_text_file(
        &self,
        args: WriteTextFileRequest,
    ) -> AcpResult<WriteTextFileResponse> {
        tracing::debug!(path = %args.path.display(), "agent writing file");
        fs_handler::write_text_file(&args.path, &args.content)
            .await
            .map_err(internal_err)?;
        Ok(WriteTextFileResponse::new())
    }

    async fn create_terminal(
        &self,
        args: CreateTerminalRequest,
    ) -> AcpResult<CreateTerminalResponse> {
        tracing::debug!(command = %args.command, "agent creating terminal");
        let env: Vec<(String, String)> = args
            .env
            .iter()
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect();
        let terminal_id = self
            .terminal_manager
            .lock()
            .await
            .create(
                &args.command,
                &args.args,
                args.cwd.as_ref(),
                &env,
                args.output_byte_limit,
            )
            .await
            .map_err(internal_err)?;
        Ok(CreateTerminalResponse::new(terminal_id))
    }

    async fn terminal_output(
        &self,
        args: TerminalOutputRequest,
    ) -> AcpResult<TerminalOutputResponse> {
        let (output, truncated, exit_status) = self
            .terminal_manager
            .lock()
            .await
            .output(&args.terminal_id.0)
            .await
            .map_err(internal_err)?;
        let mut resp = TerminalOutputResponse::new(output, truncated);
        if let Some(status) = exit_status {
            resp = resp.exit_status(status);
        }
        Ok(resp)
    }

    async fn wait_for_terminal_exit(
        &self,
        args: WaitForTerminalExitRequest,
    ) -> AcpResult<WaitForTerminalExitResponse> {
        let status = self
            .terminal_manager
            .lock()
            .await
            .wait_for_exit(&args.terminal_id.0)
            .await
            .map_err(internal_err)?;
        Ok(WaitForTerminalExitResponse::new(status))
    }

    async fn kill_terminal_command(
        &self,
        args: KillTerminalCommandRequest,
    ) -> AcpResult<KillTerminalCommandResponse> {
        self.terminal_manager
            .lock()
            .await
            .kill(&args.terminal_id.0)
            .await
            .map_err(internal_err)?;
        Ok(KillTerminalCommandResponse::new())
    }

    async fn release_terminal(
        &self,
        args: ReleaseTerminalRequest,
    ) -> AcpResult<ReleaseTerminalResponse> {
        self.terminal_manager
            .lock()
            .await
            .release(&args.terminal_id.0)
            .await
            .map_err(internal_err)?;
        Ok(ReleaseTerminalResponse::new())
    }

    async fn ext_method(&self, args: ExtRequest) -> AcpResult<ExtResponse> {
        tracing::info!(method = %args.method, "agent calling ext method");
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.event_tx
            .send(AcpEvent::ExtMethodRequest {
                request: args,
                response_tx,
            })
            .map_err(|_| {
                agent_client_protocol::Error::internal_error().data("gateway event loop shut down")
            })?;
        response_rx.await.map_err(|_| {
            agent_client_protocol::Error::internal_error()
                .data("ext method response channel dropped")
        })?
    }

    async fn ext_notification(&self, args: ExtNotification) -> AcpResult<()> {
        tracing::debug!(method = %args.method, "unhandled ext notification from agent");
        Ok(())
    }
}
