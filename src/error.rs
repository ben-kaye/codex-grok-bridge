use codex_app_server_protocol::JSONRPCErrorError;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("ACP protocol error: {0}")]
    Acp(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("translation error: {0}")]
    Translation(String),

    #[error("session not found for thread: {thread_id}")]
    SessionNotFound { thread_id: String },

    #[error("ACP subprocess exited unexpectedly")]
    SubprocessExited,

    #[error("not initialized")]
    NotInitialized,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl GatewayError {
    /// Convert to a JSON-RPC error suitable for sending to the Codex client.
    pub fn to_jsonrpc_error(&self) -> JSONRPCErrorError {
        let code = match self {
            Self::NotInitialized => -32002,
            Self::SessionNotFound { .. } => -32001,
            _ => -32603, // internal error
        };
        JSONRPCErrorError {
            code,
            message: self.to_string(),
            data: None,
        }
    }
}
