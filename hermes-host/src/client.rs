//! Host-to-Root Async Client Engine
//!
//! Manages registration, periodic heartbeats, command execution routing,
//! log streaming, and resilient automatic reconnection to the Root Management Service.

use crate::config::HostConfig;
use crate::container::ServiceContainer;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use hermes_core::models::HostInfo;
use hermes_core::protocol::{
    decode_framed_message, encode_framed_message, BackupLogsResult, ErrorCode, HermesMessage,
    RegisterHostParams, ServiceLifecycleCommand,
};
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tar::Builder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

pub struct HostClient {
    config: HostConfig,
    container: Arc<Mutex<ServiceContainer>>,
}

impl HostClient {
    pub fn new(config: HostConfig, container: Arc<Mutex<ServiceContainer>>) -> Self {
        Self { config, container }
    }

    /// Runs the client connection loop with automatic reconnection on disconnect.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let reconnect_delay = Duration::from_secs(2);

        loop {
            info!(
                root_address = %self.config.root_address,
                host_id = %self.config.id,
                "Attempting connection to Hermes Root Management Daemon"
            );

            match TcpStream::connect(&self.config.root_address).await {
                Ok(stream) => {
                    info!("Successfully connected to Hermes Root Service");
                    if let Err(e) = self.handle_session(stream).await {
                        warn!(error = %e, "Session with Root Service terminated");
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        root_address = %self.config.root_address,
                        "Failed to connect to Root Service; retrying in {:?}",
                        reconnect_delay
                    );
                }
            }

            tokio::time::sleep(reconnect_delay).await;
        }
    }

    async fn handle_session(
        &self,
        stream: TcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut read_half, mut write_half) = stream.into_split();
        let (out_tx, mut out_rx) = mpsc::channel::<HermesMessage>(64);

        // Dedicated writer task
        let writer_task = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                match encode_framed_message(&msg) {
                    Ok(bytes) => {
                        if let Err(e) = write_half.write_all(&bytes).await {
                            debug!("Write error on stream: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to encode framed message: {}", e);
                    }
                }
            }
        });

        // 1. Send Registration Request
        let host_info = self.build_host_info().await;
        let reg_req = HermesMessage::new_request(
            "host.register",
            serde_json::to_value(RegisterHostParams { host: host_info })?,
        );
        out_tx.send(reg_req).await?;

        // 2. Spawn Heartbeat Task
        let out_tx_hb = out_tx.clone();
        let host_id_hb = self.config.id.clone();
        let hb_interval = Duration::from_secs(self.config.heartbeat_interval_secs);

        let heartbeat_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(hb_interval);
            loop {
                ticker.tick().await;
                let hb_msg = HermesMessage::Heartbeat {
                    host_id: host_id_hb.clone(),
                    timestamp: Utc::now(),
                };
                if let Err(_) = out_tx_hb.send(hb_msg).await {
                    break;
                }
            }
        });

        // 3. Command Processing Loop
        let mut read_buffer = Vec::with_capacity(16 * 1024);
        let mut chunk = [0u8; 8192];

        loop {
            let n = read_half.read(&mut chunk).await?;
            if n == 0 {
                break; // EOF / Disconnect
            }
            read_buffer.extend_from_slice(&chunk[..n]);

            while !read_buffer.is_empty() {
                match decode_framed_message(&read_buffer) {
                    Ok(Some((message, consumed))) => {
                        read_buffer.drain(0..consumed);

                        self.process_incoming_command(message, &out_tx).await?;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        error!("Framing error from Root: {}", e);
                        return Err(Box::new(e));
                    }
                }
            }
        }

        heartbeat_task.abort();
        writer_task.abort();
        Ok(())
    }

    async fn process_incoming_command(
        &self,
        message: HermesMessage,
        out_tx: &mpsc::Sender<HermesMessage>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match message {
            HermesMessage::Request { id, method, params } => {
                debug!(id = %id, method = %method, "Received command from Root");
                match method.as_str() {
                    "service.command" => {
                        let service_id = params["service_id"]
                            .as_str()
                            .ok_or("Missing service_id")?;
                        let command_val = params["command"].clone();
                        let command: ServiceLifecycleCommand = serde_json::from_value(command_val)?;

                        let result = {
                            let mut container = self.container.lock().await;
                            match command {
                                ServiceLifecycleCommand::Start => container.start_service(service_id),
                                ServiceLifecycleCommand::Stop => container.stop_service(service_id),
                                ServiceLifecycleCommand::Restart => container.restart_service(service_id),
                                ServiceLifecycleCommand::Quiesce => container.stop_service(service_id),
                                ServiceLifecycleCommand::ClearFault => container.clear_fault(service_id),
                            }
                        };

                        match result {
                            Ok(svc_info) => {
                                out_tx
                                    .send(HermesMessage::new_response(
                                        id,
                                        serde_json::to_value(&svc_info)?,
                                    ))
                                    .await?;
                            }
                            Err(e) => {
                                out_tx
                                    .send(HermesMessage::new_error_response(
                                        id,
                                        ErrorCode::ExecutionFailed,
                                        e.to_string(),
                                    ))
                                    .await?;
                            }
                        }
                    }

                    "diagnostics.get_thread_stacks" => {
                        let service_id = params["service_id"]
                            .as_str()
                            .ok_or("Missing service_id")?;

                        let stacks = {
                            let container = self.container.lock().await;
                            container.get_thread_stacks(service_id)
                        };

                        match stacks {
                            Ok(stack_list) => {
                                out_tx
                                    .send(HermesMessage::new_response(
                                        id,
                                        serde_json::to_value(&stack_list)?,
                                    ))
                                    .await?;
                            }
                            Err(e) => {
                                out_tx
                                    .send(HermesMessage::new_error_response(
                                        id,
                                        ErrorCode::ExecutionFailed,
                                        e.to_string(),
                                    ))
                                    .await?;
                            }
                        }
                    }

                    "diagnostics.get_data" => {
                        let service_id = params["service_id"]
                            .as_str()
                            .ok_or("Missing service_id")?;

                        let data = {
                            let container = self.container.lock().await;
                            container.get_diagnostics(service_id)
                        };

                        match data {
                            Ok(diag_json) => {
                                out_tx
                                    .send(HermesMessage::new_response(id, diag_json))
                                    .await?;
                            }
                            Err(e) => {
                                out_tx
                                    .send(HermesMessage::new_error_response(
                                        id,
                                        ErrorCode::ExecutionFailed,
                                        e.to_string(),
                                    ))
                                    .await?;
                            }
                        }
                    }

                    "diagnostics.backup_logs" => {
                        let dest_dir = params["destination_dir"]
                            .as_str()
                            .unwrap_or("logs/backups");
                        let svc_filter = params["service_id"].as_str();

                        match self.create_log_backup(dest_dir, svc_filter).await {
                            Ok(backup_res) => {
                                out_tx
                                    .send(HermesMessage::new_response(
                                        id,
                                        serde_json::to_value(backup_res)?,
                                    ))
                                    .await?;
                            }
                            Err(e) => {
                                out_tx
                                    .send(HermesMessage::new_error_response(
                                        id,
                                        ErrorCode::ExecutionFailed,
                                        format!("Failed to create log backup: {}", e),
                                    ))
                                    .await?;
                            }
                        }
                    }

                    _ => {
                        out_tx
                            .send(HermesMessage::new_error_response(
                                id,
                                ErrorCode::UnknownMethod,
                                format!("Unknown host command: {}", method),
                            ))
                            .await?;
                    }
                }
            }

            HermesMessage::Response { id, .. } => {
                debug!(id = %id, "Received response acknowledgment from Root");
            }

            _ => {}
        }

        Ok(())
    }

    async fn build_host_info(&self) -> HostInfo {
        let pid = std::process::id();
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        let services = {
            let container = self.container.lock().await;
            container.get_topology()
        };

        HostInfo {
            id: self.config.id.clone(),
            name: self.config.name.clone(),
            hostname,
            pid,
            os: std::env::consts::OS.to_string(),
            uptime_secs: 0,
            last_heartbeat: Utc::now(),
            services,
        }
    }

    async fn create_log_backup(
        &self,
        dest_dir: &str,
        _service_filter: Option<&str>,
    ) -> Result<BackupLogsResult, Box<dyn std::error::Error + Send + Sync>> {
        create_dir_all(dest_dir)?;

        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let archive_filename = format!("hermes_logs_{}_{}.tar.gz", self.config.id, timestamp);
        let archive_path = Path::new(dest_dir).join(&archive_filename);

        let tar_gz = File::create(&archive_path)?;
        let enc = GzEncoder::new(tar_gz, Compression::default());
        let mut tar = Builder::new(enc);

        let log_dir = Path::new(&self.config.log_dir);
        let mut file_count = 0;

        if log_dir.exists() && log_dir.is_dir() {
            for entry in std::fs::read_dir(log_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    let filename = path.file_name().unwrap_or_default();
                    tar.append_path_with_name(&path, filename)?;
                    file_count += 1;
                }
            }
        }

        tar.finish()?;
        let final_size = std::fs::metadata(&archive_path)?.len();

        info!(
            archive = %archive_path.display(),
            files = file_count,
            size_bytes = final_size,
            "Successfully packaged diagnostic log backup"
        );

        Ok(BackupLogsResult {
            archive_path: archive_path.to_string_lossy().to_string(),
            file_count,
            byte_size: final_size,
            created_at: Utc::now(),
        })
    }
}
