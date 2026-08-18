use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;

/// Protocol translation gateway between Codex app-server clients and ACP agents.
#[derive(Parser, Debug, Clone)]
#[command(name = "codex-grok-bridge", about)]
pub struct GatewayConfig {
    /// Operating mode.
    ///
    /// `codex` (default): Translate between Codex app-server protocol and ACP.
    /// `acp-proxy`: Transparent WebSocket-to-stdio bridge for ACP — no Codex
    /// translation, just framing conversion so remote ACP clients can reach a
    /// local agent subprocess over WebSocket.
    #[arg(long, default_value = "codex")]
    pub mode: GatewayMode,

    /// Transport endpoint URL. Supported values: `stdio://` (default),
    /// `ws://IP:PORT`.
    #[arg(long = "listen", value_name = "URL", default_value = "stdio://")]
    pub listen: ListenTransport,

    /// Command to spawn as the ACP agent (e.g. "gemini").
    #[arg(long)]
    pub agent_cmd: String,

    /// Arguments to pass to the ACP agent command.
    /// Everything after `--` on the command line is forwarded here.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub agent_args: Vec<String>,

    /// Working directory for the ACP agent subprocess.
    #[arg(long, default_value = ".")]
    pub cwd: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

/// Gateway operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayMode {
    /// Translate between Codex app-server protocol and ACP.
    Codex,
    /// Transparent WS-to-stdio ACP proxy (no protocol translation).
    AcpProxy,
}

impl FromStr for GatewayMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "codex" => Ok(Self::Codex),
            "acp-proxy" => Ok(Self::AcpProxy),
            _ => Err(format!(
                "unknown mode: {s:?} (expected 'codex' or 'acp-proxy')"
            )),
        }
    }
}

impl std::fmt::Display for GatewayMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codex => write!(f, "codex"),
            Self::AcpProxy => write!(f, "acp-proxy"),
        }
    }
}

/// Transport mode for the Codex client side.
#[derive(Debug, Clone)]
pub enum ListenTransport {
    /// Read/write Codex NDJSON over stdin/stdout.
    Stdio,
    /// Accept WebSocket connections on the given address.
    WebSocket { bind_address: SocketAddr },
}

impl FromStr for ListenTransport {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "stdio://" {
            return Ok(Self::Stdio);
        }

        if let Some(addr_str) = s.strip_prefix("ws://") {
            let bind_address = addr_str.parse::<SocketAddr>().map_err(|_| {
                format!("invalid WebSocket listen URL: {s:?} (expected 'ws://IP:PORT', e.g. 'ws://0.0.0.0:8080')")
            })?;
            return Ok(Self::WebSocket { bind_address });
        }

        Err(format!(
            "unsupported listen URL: {s:?} (expected 'stdio://' or 'ws://IP:PORT')"
        ))
    }
}

impl std::fmt::Display for ListenTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdio => write!(f, "stdio://"),
            Self::WebSocket { bind_address } => write!(f, "ws://{bind_address}"),
        }
    }
}
