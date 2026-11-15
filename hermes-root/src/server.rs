//! Root Server Async Framed TCP Engine
//!
//! Accepts incoming connections from service hosts and desktop operators,
//! multiplexes framed JSON protocol messages, manages heartbeats, and routes RPC commands.

use crate::config::RootConfig;
use crate::event_log::EventStore;
use crate::registry::Registry;
use crate::router::CommandRouter;
use hermes_core::models::{FaultRecord, LogLevel, SystemEventType};
use hermes_core::protocol::{
    decode_framed_message, encode_framed_message, ErrorCode, HermesMessage,
    RegisterHostParams, UpdateServiceStateParams,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info};

pub struct RootServer {
    config: RootConfig,
    registry: Arc<Mutex<Registry>>,
    event_store: Arc<Mutex<EventStore>>,
    router: Arc<CommandRouter>,
}

impl RootServer {
    pub fn new(config: RootConfig) -> Self {
        let registry = Arc::new(Mutex::new(Registry::new()));
        let event_store = Arc::new(Mutex::new(EventStore::new(
            config.storage.event_log_capacity,
            config.storage.log_history_capacity,
            if config.storage.persist_events {
                Some(config.storage.events_file.clone())
            } else {
                None
            },
        )));
        let router = Arc::new(CommandRouter::new(Duration::from_secs(
            config.server.heartbeat_timeout_secs,
        )));

        Self {
            config,
            registry,
            event_store,
            router,
        }
    }

    /// Access the shared in-memory registry.
    pub fn registry(&self) -> Arc<Mutex<Registry>> {
        self.registry.clone()
    }

    /// Access the shared event store.
    pub fn event_store(&self) -> Arc<Mutex<EventStore>> {
        self.event_store.clone()
    }

    /// Starts the TCP listener on the configured bind address and runs the server.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.config.server.bind_address).await?;
        self.run_with_listener(listener).await
    }

    /// Runs the server using an existing bound TCP listener.
    pub async fn run_with_listener(
        &self,
        listener: TcpListener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let local_addr = listener.local_addr()?;
        info!(
            bind_address = %local_addr,
            "Hermes Root Management Daemon listening"
        );

        // Spawn background heartbeat monitor task
        let registry_clone = self.registry.clone();
        let event_store_clone = self.event_store.clone();
        let timeout = Duration::from_secs(self.config.server.heartbeat_timeout_secs);
        let check_interval = Duration::from_secs(self.config.server.heartbeat_check_interval_secs);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(check_interval);
            loop {
                ticker.tick().await;
                let expired = {
                    let mut reg = registry_clone.lock().await;
                    reg.check_stale_hosts(timeout)
                };

                for host_id in expired {
                    let evt = {
                        let mut store = event_store_clone.lock().await;
                        store.emit_event(
                            SystemEventType::HostDisconnected,
                            &host_id,
                            None,
                            format!("Host '{}' disconnected (heartbeat timed out)", host_id),
                            serde_json::Value::Null,
                        )
                    };

                    let mut reg = registry_clone.lock().await;
                    reg.broadcast_to_clients(HermesMessage::Event(evt));
                }
            }
        });

        // Accept connection loop
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!(peer = %addr, "New TCP connection established");
                    let registry = self.registry.clone();
                    let event_store = self.event_store.clone();
                    let router = self.router.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, registry, event_store, router).await {
                            debug!(peer = %addr, error = %e, "Connection closed");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept TCP connection");
                }
            }
        }
    }
}

enum ConnectionRole {
    Unidentified,
    Host(String),
    Client,
}

async fn handle_connection(
    stream: TcpStream,
    registry: Arc<Mutex<Registry>>,
    event_store: Arc<Mutex<EventStore>>,
    router: Arc<CommandRouter>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut read_half, mut write_half) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::channel::<HermesMessage>(128);

    // Dedicated writer task for framed messages
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

    let mut read_buffer = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 8192];
    let mut role = ConnectionRole::Unidentified;

    loop {
        let n = read_half.read(&mut chunk).await?;
        if n == 0 {
            break; // Connection closed
        }
        read_buffer.extend_from_slice(&chunk[..n]);

        // Decode as many frames as available
        while !read_buffer.is_empty() {
            match decode_framed_message(&read_buffer) {
                Ok(Some((message, consumed))) => {
                    read_buffer.drain(0..consumed);

                    process_message(
                        message,
                        &out_tx,
                        &mut role,
                        &registry,
                        &event_store,
                        &router,
                    )
                    .await?;
                }
                Ok(None) => break, // Incomplete frame, wait for more data
                Err(e) => {
                    error!("Framing decoding error: {}", e);
                    return Err(Box::new(e));
                }
            }
        }
    }

    // Cleanup on disconnect
    if let ConnectionRole::Host(ref host_id) = role {
        info!(host_id = %host_id, "Host connection terminated");
        let evt = {
            let mut store = event_store.lock().await;
            store.emit_event(
                SystemEventType::HostDisconnected,
                host_id,
                None,
                format!("Host '{}' disconnected", host_id),
                serde_json::Value::Null,
            )
        };

        let mut reg = registry.lock().await;
        reg.unregister_host(host_id);
        reg.broadcast_to_clients(HermesMessage::Event(evt));
    }

    writer_task.abort();
    Ok(())
}

async fn process_message(
    message: HermesMessage,
    out_tx: &mpsc::Sender<HermesMessage>,
    role: &mut ConnectionRole,
    registry: &Arc<Mutex<Registry>>,
    event_store: &Arc<Mutex<EventStore>>,
    router: &Arc<CommandRouter>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match message {
        HermesMessage::Request {
            ref id,
            ref method,
            ref params,
        } => {
            handle_rpc_request(
                id,
                method,
                params,
                out_tx,
                role,
                registry,
                event_store,
                router,
            )
            .await?;
        }

        HermesMessage::Response { ref id, .. } => {
            let req_id = id.clone();
            // Forward response to router to complete pending command
            router.complete_request(&req_id, message).await;
        }

        HermesMessage::Heartbeat {
            ref host_id,
            timestamp,
        } => {
            let mut reg = registry.lock().await;
            reg.record_heartbeat(host_id);
            debug!(host_id = %host_id, timestamp = %timestamp, "Heartbeat handled");
        }

        HermesMessage::Event(event) => {
            // Record event and broadcast to all connected operators
            {
                let mut store = event_store.lock().await;
                store.record_event(event.clone());
            }

            // If it's a fault, update registry
            if event.event_type == SystemEventType::FaultDetected {
                if let Ok(fault) = serde_json::from_value::<FaultRecord>(event.metadata.clone()) {
                    let mut reg = registry.lock().await;
                    reg.record_fault(fault);
                }
            }

            let mut reg = registry.lock().await;
            reg.broadcast_to_clients(HermesMessage::Event(event));
        }

        HermesMessage::Log(log) => {
            {
                let mut store = event_store.lock().await;
                store.record_log(log.clone());
            }

            let mut reg = registry.lock().await;
            reg.broadcast_to_clients(HermesMessage::Log(log));
        }
    }

    Ok(())
}

async fn handle_rpc_request(
    id: &str,
    method: &str,
    params: &serde_json::Value,
    out_tx: &mpsc::Sender<HermesMessage>,
    role: &mut ConnectionRole,
    registry: &Arc<Mutex<Registry>>,
    event_store: &Arc<Mutex<EventStore>>,
    router: &Arc<CommandRouter>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match method {
        // Host Registration
        "host.register" => {
            if let Ok(reg_params) = serde_json::from_value::<RegisterHostParams>(params.clone()) {
                let host_info = reg_params.host;
                let host_id = host_info.id.clone();

                *role = ConnectionRole::Host(host_id.clone());

                let evt = {
                    let mut store = event_store.lock().await;
                    store.emit_event(
                        SystemEventType::HostRegistered,
                        &host_id,
                        None,
                        format!("Host '{}' registered with {} services", host_info.name, host_info.services.len()),
                        serde_json::to_value(&host_info)?,
                    )
                };

                {
                    let mut reg = registry.lock().await;
                    reg.register_host(host_info, out_tx.clone());
                    reg.broadcast_to_clients(HermesMessage::Event(evt));
                }

                out_tx
                    .send(HermesMessage::new_response(
                        id,
                        serde_json::json!({
                            "status": "registered",
                            "host_id": host_id,
                            "protocol": "hermes.v1"
                        }),
                    ))
                    .await?;
            } else {
                out_tx
                    .send(HermesMessage::new_error_response(
                        id,
                        ErrorCode::InvalidParams,
                        "Invalid host.register parameters",
                    ))
                    .await?;
            }
        }

        // Host reporting service state change
        "host.update_service_state" => {
            if let Ok(state_params) = serde_json::from_value::<UpdateServiceStateParams>(params.clone()) {
                let updated = {
                    let mut reg = registry.lock().await;
                    reg.update_service_state(
                        &state_params.host_id,
                        &state_params.service_id,
                        state_params.state,
                        state_params.health_summary,
                    )
                };

                if let Some(svc) = updated {
                    let evt = {
                        let mut store = event_store.lock().await;
                        store.emit_event(
                            SystemEventType::ServiceStarted,
                            &state_params.host_id,
                            Some(state_params.service_id.clone()),
                            format!("Service '{}' transitioned to {}", svc.name, state_params.state),
                            serde_json::to_value(&svc)?,
                        )
                    };

                    let mut reg = registry.lock().await;
                    reg.broadcast_to_clients(HermesMessage::Event(evt));

                    out_tx
                        .send(HermesMessage::new_response(
                            id,
                            serde_json::json!({ "success": true }),
                        ))
                        .await?;
                } else {
                    out_tx
                        .send(HermesMessage::new_error_response(
                            id,
                            ErrorCode::ServiceNotFound,
                            "Service not found for state update",
                        ))
                        .await?;
                }
            } else {
                out_tx
                    .send(HermesMessage::new_error_response(
                        id,
                        ErrorCode::InvalidParams,
                        "Invalid host.update_service_state parameters",
                    ))
                    .await?;
            }
        }

        // Desktop Operator subscribing to live events/logs
        "client.subscribe" => {
            *role = ConnectionRole::Client;
            {
                let mut reg = registry.lock().await;
                reg.add_subscriber(out_tx.clone());
            }
            out_tx
                .send(HermesMessage::new_response(
                    id,
                    serde_json::json!({ "status": "subscribed" }),
                ))
                .await?;
        }

        // Desktop querying complete topology
        "root.query_topology" => {
            let hosts = {
                let reg = registry.lock().await;
                reg.get_topology()
            };
            out_tx
                .send(HermesMessage::new_response(
                    id,
                    serde_json::json!({ "hosts": hosts }),
                ))
                .await?;
        }

        // Desktop querying event history
        "root.query_events" => {
            let limit = params["limit"].as_u64().unwrap_or(50) as usize;
            let offset = params["offset"].as_u64().unwrap_or(0) as usize;
            let filter_host = params["host_id"].as_str();
            let filter_service = params["service_id"].as_str();

            let events = {
                let store = event_store.lock().await;
                store.query_events(limit, offset, filter_host, filter_service)
            };

            out_tx
                .send(HermesMessage::new_response(
                    id,
                    serde_json::json!({ "events": events }),
                ))
                .await?;
        }

        // Desktop querying log history
        "root.query_logs" => {
            let limit = params["limit"].as_u64().unwrap_or(100) as usize;
            let filter_level = params["min_level"]
                .as_str()
                .and_then(|l| match l.to_uppercase().as_str() {
                    "TRACE" => Some(LogLevel::Trace),
                    "DEBUG" => Some(LogLevel::Debug),
                    "INFO" => Some(LogLevel::Info),
                    "WARN" => Some(LogLevel::Warn),
                    "ERROR" => Some(LogLevel::Error),
                    _ => None,
                });
            let filter_service = params["service_id"].as_str();
            let search_text = params["search"].as_str();

            let logs = {
                let store = event_store.lock().await;
                store.query_logs(limit, filter_level, filter_service, search_text)
            };

            out_tx
                .send(HermesMessage::new_response(
                    id,
                    serde_json::json!({ "logs": logs }),
                ))
                .await?;
        }

        // Routed commands to hosts (service.command, diagnostics.get_thread_stacks, diagnostics.backup_logs)
        "service.command" | "diagnostics.get_thread_stacks" | "diagnostics.backup_logs" | "diagnostics.get_data" => {
            let host_id = params["host_id"]
                .as_str()
                .ok_or("Missing host_id in command params")?;

            let host_sender = {
                let reg = registry.lock().await;
                reg.get_host_sender(host_id)
            };

            if let Some(sender) = host_sender {
                // Record operator intervention event
                if method == "service.command" {
                    let svc_id = params["service_id"].as_str().map(|s| s.to_string());
                    let cmd_name = params["command"].as_str().unwrap_or("unknown");
                    let mut store = event_store.lock().await;
                    store.emit_event(
                        SystemEventType::OperatorIntervention,
                        host_id,
                        svc_id,
                        format!("Operator issued '{}' command", cmd_name),
                        params.clone(),
                    );
                }

                // Dispatch to host via router and wait for matching response
                let forward_msg = HermesMessage::Request {
                    id: id.to_string(),
                    method: method.to_string(),
                    params: params.clone(),
                };

                let response = router
                    .dispatch_to_host(id.to_string(), sender, forward_msg)
                    .await;

                out_tx.send(response).await?;
            } else {
                out_tx
                    .send(HermesMessage::new_error_response(
                        id,
                        ErrorCode::HostNotFound,
                        format!("Host '{}' is not connected or unavailable", host_id),
                    ))
                    .await?;
            }
        }

        _ => {
            out_tx
                .send(HermesMessage::new_error_response(
                    id,
                    ErrorCode::UnknownMethod,
                    format!("Unknown RPC method: {}", method),
                ))
                .await?;
        }
    }

    Ok(())
}
