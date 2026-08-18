use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use codex_app_server_protocol::{
    JSONRPCError, JSONRPCErrorError, JSONRPCMessage, JSONRPCNotification, JSONRPCRequest,
    JSONRPCResponse, RequestId,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

/// Sends JSON-RPC messages to the Codex process via the stdio writer task.
///
/// Wraps an `mpsc::Sender<String>` that feeds pre-serialized JSON to the
/// writer. Also manages pending server-initiated requests so callers can
/// `await` the response.
#[derive(Clone)]
pub struct OutgoingMessageSender {
    tx: mpsc::Sender<String>,
    next_id: Arc<AtomicI64>,
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<JSONRPCResponse>>>>,
}

impl OutgoingMessageSender {
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self {
            tx,
            next_id: Arc::new(AtomicI64::new(1)),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Send a successful response for the given request id.
    pub async fn send_response(
        &self,
        id: RequestId,
        result: serde_json::Value,
    ) -> Result<(), SendError> {
        let msg = JSONRPCMessage::Response(JSONRPCResponse { id, result });
        self.send_raw(msg).await
    }

    /// Send an error response for the given request id.
    pub async fn send_error(
        &self,
        id: RequestId,
        error: JSONRPCErrorError,
    ) -> Result<(), SendError> {
        let msg = JSONRPCMessage::Error(JSONRPCError { id, error });
        self.send_raw(msg).await
    }

    /// Send a notification (no response expected).
    pub async fn send_notification(
        &self,
        method: impl Into<String>,
        params: Option<serde_json::Value>,
    ) -> Result<(), SendError> {
        let msg = JSONRPCMessage::Notification(JSONRPCNotification {
            method: method.into(),
            params,
        });
        self.send_raw(msg).await
    }

    /// Send a request to the Codex client and return a receiver for the
    /// response. The caller should `await` the receiver to get the
    /// `JSONRPCResponse` once the client replies.
    pub async fn send_request(
        &self,
        method: impl Into<String>,
        params: Option<serde_json::Value>,
    ) -> Result<oneshot::Receiver<JSONRPCResponse>, SendError> {
        let id = RequestId::Integer(self.next_id.fetch_add(1, Ordering::Relaxed));
        let (resp_tx, resp_rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock().await;
            pending.insert(id.clone(), resp_tx);
        }

        let msg = JSONRPCMessage::Request(JSONRPCRequest {
            id: id.clone(),
            method: method.into(),
            params,
        });

        if let Err(e) = self.send_raw(msg).await {
            // Clean up the pending entry on send failure.
            let mut pending = self.pending.lock().await;
            pending.remove(&id);
            return Err(e);
        }

        Ok(resp_rx)
    }

    /// Called by the message dispatcher when a response arrives from the
    /// Codex client. Resolves the corresponding `oneshot` so the original
    /// `send_request` caller receives the response.
    pub async fn resolve_pending(&self, response: JSONRPCResponse) {
        let mut pending = self.pending.lock().await;
        if let Some(tx) = pending.remove(&response.id) {
            let _ = tx.send(response);
        } else {
            warn!(id = ?response.id, "received response for unknown request id");
        }
    }

    /// Serialize a `JSONRPCMessage` and push it to the writer channel.
    async fn send_raw(&self, message: JSONRPCMessage) -> Result<(), SendError> {
        let json = serde_json::to_string(&message).map_err(SendError::Serialize)?;
        self.tx
            .send(json)
            .await
            .map_err(|_| SendError::ChannelClosed)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("failed to serialize message")]
    Serialize(#[source] serde_json::Error),
    #[error("writer channel closed")]
    ChannelClosed,
}
