use std::ffi::OsString;

use clap::Parser;
use codex_acp_gateway::config::GatewayConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let raw_args: Vec<OsString> = std::env::args_os().collect();

    // The Codex desktop invokes CODEX_CLI_PATH as if it were the Codex CLI,
    // with global flags followed by `app-server`. Detect that shape before
    // clap parses the gateway's developer-facing command line.
    if raw_args.iter().skip(1).any(|arg| arg == "app-server") {
        init_tracing("warn");
        if let Err(error) = codex_acp_gateway::hybrid::run_from_codex_cli_args(&raw_args[1..]).await
        {
            tracing::error!("bridge exited with error: {error:#}");
            std::process::exit(1);
        }
        return;
    }

    if raw_args.len() == 2 && matches!(raw_args[1].to_str(), Some("--version" | "-V")) {
        match codex_acp_gateway::hybrid::native_codex_version().await {
            Ok(version) => println!("{version}"),
            Err(error) => {
                eprintln!("failed to query bundled Codex version: {error:#}");
                std::process::exit(1);
            }
        }
        return;
    }

    let config = GatewayConfig::parse();

    init_tracing(&config.log_level);

    // Detect bwrap availability once at startup (result is cached).
    codex_acp_gateway::sandbox::check_availability();

    let result = match config.mode {
        codex_acp_gateway::config::GatewayMode::Codex => codex_acp_gateway::run(config).await,
        codex_acp_gateway::config::GatewayMode::AcpProxy => {
            codex_acp_gateway::acp_proxy::run(&config).await
        }
    };

    if let Err(e) = result {
        tracing::error!("gateway exited with error: {e:#}");
        std::process::exit(1);
    }
}

fn init_tracing(default_level: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level)),
        )
        .with_writer(std::io::stderr)
        .init();
}
