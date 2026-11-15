//! Hermes Service Host Executable
//!
//! Reusable container executable capable of hosting multiple dynamic service libraries
//! driven by independent configuration files.

use hermes_host::{HostClient, HostFileConfig, ServiceContainer};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    // Parse command line arguments
    let mut config_path: Option<PathBuf> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            if let Some(path) = args.next() {
                config_path = Some(PathBuf::from(path));
            }
        }
    }

    let config_file = match config_path {
        Some(path) => path,
        None => {
            eprintln!("Usage: hermes-host --config <path-to-toml-config>");
            eprintln!("Example: hermes-host --config configs/host-single-telemetry.toml");
            std::process::exit(1);
        }
    };

    info!(
        config = %config_file.display(),
        "Starting Hermes Service Host Container"
    );

    let host_config = HostFileConfig::from_file(&config_file).unwrap_or_else(|e| {
        error!(error = %e, "Failed to load host configuration file");
        std::process::exit(1);
    });

    info!(
        host_id = %host_config.host.id,
        host_name = %host_config.host.name,
        plugins_count = host_config.plugins.len(),
        "Host configuration loaded successfully"
    );

    // Initialize Service Container
    let mut container = ServiceContainer::new(&host_config.host.id);
    if let Err(e) = container.initialize_plugins(&host_config.plugins) {
        error!(error = %e, "Failed to initialize dynamic plugins");
        std::process::exit(1);
    }

    // Start all configured services
    if let Err(e) = container.start_all() {
        error!(error = %e, "Failed to start configured services");
    }

    let container_arc = Arc::new(Mutex::new(container));

    // Install global process panic hook to catch and quarantine any unexpected thread failure
    let (fault_tx, mut fault_rx) = tokio::sync::mpsc::channel(32);
    hermes_host::fault_manager::install_process_panic_hook(
        host_config.host.id.clone(),
        fault_tx,
    );

    let container_fault_ref = container_arc.clone();
    tokio::spawn(async move {
        while let Some(fault) = fault_rx.recv().await {
            let mut cont = container_fault_ref.lock().await;
            let svc_id = fault.service_id.clone();
            let _ = cont.record_fault(&svc_id, fault);
        }
    });

    let client = HostClient::new(host_config.host.clone(), container_arc.clone());

    // Run client loop with graceful termination
    tokio::select! {
        res = client.run() => {
            if let Err(e) = res {
                error!("Host client terminated with error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received (Ctrl+C). Quiescing and stopping all hosted services...");
            let mut cont = container_arc.lock().await;
            let _ = cont.stop_all();
            info!("All services stopped cleanly. Host process exiting.");
        }
    }

    Ok(())
}
