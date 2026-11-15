//! Hermes Root Management Service Daemon Binary
//!
//! Central coordinator and discovery hub for Hermes Service Hosts and Desktop Operator UI.

use hermes_root::{RootConfig, RootServer};
use std::env;
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    info!("Initializing Hermes Root Management Service v0.1.0");

    // Parse --config argument if provided
    let mut config_path: Option<PathBuf> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            if let Some(path) = args.next() {
                config_path = Some(PathBuf::from(path));
            }
        }
    }

    let config = if let Some(path) = config_path {
        info!("Loading configuration from {}", path.display());
        RootConfig::from_file(&path).unwrap_or_else(|e| {
            error!("Failed to parse config file: {}. Falling back to defaults.", e);
            RootConfig::default()
        })
    } else if std::path::Path::new("configs/root.toml").exists() {
        info!("Loading default configuration from configs/root.toml");
        RootConfig::from_file("configs/root.toml").unwrap_or_default()
    } else {
        info!("Using default in-memory configuration (bind: 127.0.0.1:9090)");
        RootConfig::default()
    };

    let server = RootServer::new(config);

    // Run server with graceful shutdown signal
    tokio::select! {
        res = server.run() => {
            if let Err(e) = res {
                error!("Server terminated with error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal (Ctrl+C). Terminating Hermes Root Daemon gracefully.");
        }
    }

    Ok(())
}
