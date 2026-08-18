pub mod client_impl;
pub mod fs_handler;
pub mod spawn;

use std::path::Path;

use agent_client_protocol::ClientSideConnection;
use anyhow::{Context, Result};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub use client_impl::{AcpEvent, GatewayAcpClient};

/// A live connection to an ACP agent subprocess.
///
/// Holds the subprocess handle and provides access to the agent via the
/// [`Agent`] trait (through the `ClientSideConnection`).
pub struct AcpConnection {
    /// The subprocess running the ACP agent.
    pub child: tokio::process::Child,
    /// The client-side connection that implements the `Agent` trait.
    pub agent: ClientSideConnection,
}

/// Spawn an ACP agent subprocess and establish a JSON-RPC connection over its
/// stdin/stdout.
///
/// This must be called from within a `tokio::task::LocalSet` because the ACP
/// SDK uses `!Send` futures internally.
///
/// Returns an `AcpConnection` plus the channel receiver for events forwarded
/// by our `Client` implementation.
pub async fn connect(
    cmd: &str,
    args: &[String],
    cwd: &Path,
) -> Result<(
    AcpConnection,
    tokio::sync::mpsc::UnboundedReceiver<AcpEvent>,
)> {
    let mut child = spawn::spawn_subprocess(cmd, args, cwd)?;

    let child_stdout = child
        .stdout
        .take()
        .context("ACP agent subprocess has no stdout")?;
    let child_stdin = child
        .stdin
        .take()
        .context("ACP agent subprocess has no stdin")?;

    // Convert tokio AsyncRead/AsyncWrite to futures AsyncRead/AsyncWrite via
    // tokio-util's compat layer.
    let incoming = child_stdout.compat();
    let outgoing = child_stdin.compat_write();

    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = GatewayAcpClient::new(event_tx);

    let (connection, io_future) = ClientSideConnection::new(client, outgoing, incoming, |fut| {
        tokio::task::spawn_local(fut);
    });

    // Spawn the I/O loop on the local task set.
    tokio::task::spawn_local(async move {
        if let Err(e) = io_future.await {
            tracing::error!("ACP I/O loop ended with error: {e:?}");
        }
    });

    Ok((
        AcpConnection {
            child,
            agent: connection,
        },
        event_rx,
    ))
}
