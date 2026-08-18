use codex_app_server_protocol::JSONRPCMessage;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use super::TransportEvent;

/// Spawn a task that reads NDJSON lines from stdin and parses each as a
/// `JSONRPCMessage`. Parsed messages are forwarded via `tx`; on EOF or
/// fatal error the task sends `TransportEvent::Disconnected` and exits.
pub fn spawn_reader(tx: mpsc::Sender<TransportEvent>) {
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    debug!("stdin EOF");
                    let _ = tx.send(TransportEvent::Disconnected).await;
                    return;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<JSONRPCMessage>(trimmed) {
                        Ok(msg) => {
                            if tx
                                .send(TransportEvent::MessageReceived { message: msg })
                                .await
                                .is_err()
                            {
                                debug!("reader: receiver dropped, shutting down");
                                return;
                            }
                        }
                        Err(e) => {
                            warn!(line = trimmed, error = %e, "failed to parse JSON-RPC message");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "stdin read error");
                    let _ = tx.send(TransportEvent::Disconnected).await;
                    return;
                }
            }
        }
    });
}

/// Spawn a task that receives pre-serialized JSON strings from `rx` and
/// writes each as a line to stdout, flushing after every write.
pub fn spawn_writer(mut rx: mpsc::Receiver<String>) {
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();

        while let Some(json) = rx.recv().await {
            if let Err(e) = async {
                stdout.write_all(json.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
                Ok::<(), std::io::Error>(())
            }
            .await
            {
                error!(error = %e, "stdout write error, stopping writer");
                return;
            }
        }

        debug!("writer channel closed, shutting down");
    });
}
