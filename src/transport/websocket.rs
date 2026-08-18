use codex_app_server_protocol::JSONRPCMessage;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, warn};

use super::TransportEvent;

/// Spawn a task that reads WebSocket frames and forwards parsed JSON-RPC
/// messages via `tx`. On close/EOF it sends `TransportEvent::Disconnected`.
///
/// Unlike the stdio reader, WebSocket frames already have message boundaries
/// so we don't need NDJSON line splitting — each `Text` frame is one JSON
/// object.
///
/// Ping frames are forwarded to `pong_tx` so the writer can reply with Pong.
pub fn spawn_reader(
    tx: mpsc::Sender<TransportEvent>,
    mut ws_read: SplitStream<WebSocketStream<TcpStream>>,
    pong_tx: mpsc::Sender<Vec<u8>>,
) {
    tokio::spawn(async move {
        while let Some(frame) = ws_read.next().await {
            match frame {
                Ok(Message::Text(text)) => match serde_json::from_str::<JSONRPCMessage>(&text) {
                    Ok(msg) => {
                        if tx
                            .send(TransportEvent::MessageReceived { message: msg })
                            .await
                            .is_err()
                        {
                            debug!("ws reader: receiver dropped, shutting down");
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to parse WebSocket JSON-RPC message");
                    }
                },
                Ok(Message::Ping(data)) => {
                    let _ = pong_tx.send(data.to_vec()).await;
                }
                Ok(Message::Close(_)) => {
                    debug!("WebSocket close frame received");
                    let _ = tx.send(TransportEvent::Disconnected).await;
                    return;
                }
                Ok(Message::Pong(_) | Message::Binary(_) | Message::Frame(_)) => {}
                Err(e) => {
                    error!(error = %e, "WebSocket read error");
                    let _ = tx.send(TransportEvent::Disconnected).await;
                    return;
                }
            }
        }

        debug!("WebSocket stream ended");
        let _ = tx.send(TransportEvent::Disconnected).await;
    });
}

/// Spawn a task that receives pre-serialized JSON strings from `rx` and
/// sends each as a WebSocket `Text` frame. Also drains a `pong_rx` channel
/// to reply to client pings.
pub fn spawn_writer(
    mut rx: mpsc::Receiver<String>,
    mut ws_write: SplitSink<WebSocketStream<TcpStream>, Message>,
    mut pong_rx: mpsc::Receiver<Vec<u8>>,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                json = rx.recv() => {
                    match json {
                        Some(json) => {
                            if let Err(e) = ws_write.send(Message::Text(json.into())).await {
                                error!(error = %e, "WebSocket write error, stopping writer");
                                return;
                            }
                        }
                        None => {
                            let _ = ws_write.send(Message::Close(None)).await;
                            debug!("writer channel closed, sent WebSocket close frame");
                            return;
                        }
                    }
                }
                pong_data = pong_rx.recv() => {
                    if let Some(data) = pong_data
                        && let Err(e) = ws_write.send(Message::Pong(data.into())).await
                    {
                        error!(error = %e, "failed to send pong");
                    }
                }
            }
        }
    });
}
