pub mod outgoing;
pub mod stdio;
pub mod websocket;

use codex_app_server_protocol::JSONRPCMessage;

/// Events produced by the Codex transport reader.
#[derive(Debug)]
pub enum TransportEvent {
    /// A complete JSON-RPC message was received from the Codex process.
    MessageReceived { message: JSONRPCMessage },
    /// The transport connection was lost (EOF or fatal I/O error).
    Disconnected,
}
