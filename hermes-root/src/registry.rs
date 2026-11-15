//! Host & Service Registry
//!
//! Maintains cluster topology, host connectivity channels, service states,
//! parent-child relationships, and broadcasts updates to connected desktop clients.

use chrono::{DateTime, Utc};
use hermes_core::models::{FaultRecord, HostInfo, ServiceInfo};
use hermes_core::protocol::HermesMessage;
use hermes_core::state::ServiceState;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Connection status of a registered host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostStatus {
    Connected,
    Stale,
    Disconnected,
}

/// In-memory host record holding metadata and active communication channel.
pub struct HostRecord {
    pub info: HostInfo,
    pub sender: mpsc::Sender<HermesMessage>,
    pub last_heartbeat: DateTime<Utc>,
    pub status: HostStatus,
}

pub struct Registry {
    hosts: HashMap<String, HostRecord>,
    client_subscribers: Vec<mpsc::Sender<HermesMessage>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            hosts: HashMap::new(),
            client_subscribers: Vec::new(),
        }
    }

    /// Registers a newly connected host with its initial topology and channel.
    pub fn register_host(
        &mut self,
        info: HostInfo,
        sender: mpsc::Sender<HermesMessage>,
    ) {
        let host_id = info.id.clone();
        let last_hb = info.last_heartbeat;

        info!(
            host_id = %host_id,
            host_name = %info.name,
            service_count = info.services.len(),
            "Registered service host"
        );

        let record = HostRecord {
            info,
            sender,
            last_heartbeat: last_hb,
            status: HostStatus::Connected,
        };

        self.hosts.insert(host_id, record);
    }

    /// Records a heartbeat signal for a host, updating timestamps.
    pub fn record_heartbeat(&mut self, host_id: &str) -> bool {
        if let Some(record) = self.hosts.get_mut(host_id) {
            let now = Utc::now();
            record.last_heartbeat = now;
            record.info.last_heartbeat = now;
            record.status = HostStatus::Connected;
            debug!(host_id = %host_id, "Heartbeat recorded");
            true
        } else {
            false
        }
    }

    /// Updates the operational state of a hosted service.
    pub fn update_service_state(
        &mut self,
        host_id: &str,
        service_id: &str,
        new_state: ServiceState,
        health_summary: Option<String>,
    ) -> Option<ServiceInfo> {
        let record = self.hosts.get_mut(host_id)?;
        let service = record.info.services.iter_mut().find(|s| s.id == service_id)?;

        let old_state = service.state;
        service.state = new_state;
        service.last_state_change = Utc::now();
        if let Some(summary) = health_summary {
            service.health_summary = summary;
        }

        info!(
            host_id = %host_id,
            service_id = %service_id,
            from = %old_state,
            to = %new_state,
            "Service state transitioned"
        );

        Some(service.clone())
    }

    /// Records a service fault and attaches it to the service record.
    pub fn record_fault(&mut self, fault: FaultRecord) -> Option<ServiceInfo> {
        let record = self.hosts.get_mut(&fault.host_id)?;
        let service = record
            .info
            .services
            .iter_mut()
            .find(|s| s.id == fault.service_id)?;

        service.state = ServiceState::FaultDetected;
        service.last_state_change = Utc::now();
        service.last_fault = Some(fault);

        Some(service.clone())
    }

    /// Returns the active message sender channel for a host.
    pub fn get_host_sender(&self, host_id: &str) -> Option<mpsc::Sender<HermesMessage>> {
        self.hosts
            .get(host_id)
            .filter(|r| r.status == HostStatus::Connected)
            .map(|r| r.sender.clone())
    }

    /// Returns the complete current cluster topology.
    pub fn get_topology(&self) -> Vec<HostInfo> {
        self.hosts.values().map(|r| r.info.clone()).collect()
    }

    /// Checks for hosts that have missed heartbeats and marks them and their services as Unavailable.
    /// Returns the list of expired host IDs.
    pub fn check_stale_hosts(&mut self, timeout: Duration) -> Vec<String> {
        let now = Utc::now();
        let timeout_chrono = chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::seconds(6));
        let mut expired_hosts = Vec::new();

        for (id, record) in self.hosts.iter_mut() {
            if record.status == HostStatus::Connected {
                let age = now.signed_duration_since(record.last_heartbeat);
                if age > timeout_chrono {
                    warn!(
                        host_id = %id,
                        last_heartbeat = %record.last_heartbeat,
                        "Host heartbeat timed out; marking unavailable"
                    );
                    record.status = HostStatus::Disconnected;

                    // Transition all hosted services to Unavailable
                    for svc in record.info.services.iter_mut() {
                        svc.state = ServiceState::Unavailable;
                        svc.last_state_change = now;
                    }

                    expired_hosts.push(id.clone());
                }
            }
        }

        expired_hosts
    }

    /// Removes or unregisters a disconnected host.
    pub fn unregister_host(&mut self, host_id: &str) {
        if let Some(mut record) = self.hosts.remove(host_id) {
            record.status = HostStatus::Disconnected;
            for svc in record.info.services.iter_mut() {
                svc.state = ServiceState::Unavailable;
            }
            info!(host_id = %host_id, "Host unregistered");
        }
    }

    /// Adds a desktop client subscriber for broadcast notifications.
    pub fn add_subscriber(&mut self, sender: mpsc::Sender<HermesMessage>) {
        self.client_subscribers.push(sender);
    }

    /// Broadcasts a message to all active client subscribers, dropping dead channels.
    pub fn broadcast_to_clients(&mut self, message: HermesMessage) {
        self.client_subscribers.retain(|sender| {
            match sender.try_send(message.clone()) {
                Ok(_) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Channel full; keep subscriber but drop frame to prevent stalling root
                    warn!("Client subscriber channel full; dropping broadcast frame");
                    true
                }
            }
        });
    }

    /// Number of active registered hosts.
    pub fn active_host_count(&self) -> usize {
        self.hosts
            .values()
            .filter(|h| h.status == HostStatus::Connected)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_host(id: &str) -> HostInfo {
        HostInfo {
            id: id.to_string(),
            name: format!("Host {}", id),
            hostname: format!("{}.local", id),
            pid: 1001,
            os: "linux".to_string(),
            uptime_secs: 100,
            last_heartbeat: Utc::now(),
            services: vec![ServiceInfo {
                id: "svc-alpha".to_string(),
                name: "Alpha Service".to_string(),
                host_id: id.to_string(),
                plugin_path: "/lib/alpha.so".to_string(),
                state: ServiceState::Running,
                uptime_secs: 100,
                thread_count: 2,
                parent_service: None,
                child_services: vec![],
                health_summary: "Nominal".to_string(),
                last_fault: None,
                last_state_change: Utc::now(),
            }],
        }
    }

    #[test]
    fn test_host_registration_and_topology() {
        let mut registry = Registry::new();
        let (tx, _rx) = mpsc::channel(10);

        registry.register_host(create_test_host("host-1"), tx);
        assert_eq!(registry.active_host_count(), 1);

        let topo = registry.get_topology();
        assert_eq!(topo.len(), 1);
        assert_eq!(topo[0].id, "host-1");
        assert_eq!(topo[0].services[0].state, ServiceState::Running);
    }

    #[test]
    fn test_stale_host_expiration() {
        let mut registry = Registry::new();
        let (tx, _rx) = mpsc::channel(10);
        let mut host = create_test_host("host-stale");
        // Set last heartbeat to 10 seconds ago
        host.last_heartbeat = Utc::now() - chrono::Duration::seconds(10);

        registry.register_host(host, tx);

        // Check stale with 2 second timeout
        let expired = registry.check_stale_hosts(Duration::from_secs(2));
        assert_eq!(expired, vec!["host-stale".to_string()]);
        assert_eq!(registry.active_host_count(), 0);

        // Verify service state was set to Unavailable
        let topo = registry.get_topology();
        assert_eq!(topo[0].services[0].state, ServiceState::Unavailable);
    }

    #[test]
    fn test_service_state_update() {
        let mut registry = Registry::new();
        let (tx, _rx) = mpsc::channel(10);
        registry.register_host(create_test_host("host-1"), tx);

        let updated = registry.update_service_state(
            "host-1",
            "svc-alpha",
            ServiceState::Stopping,
            Some("Shutting down".to_string()),
        );

        assert!(updated.is_some());
        assert_eq!(updated.unwrap().state, ServiceState::Stopping);

        let topo = registry.get_topology();
        assert_eq!(topo[0].services[0].state, ServiceState::Stopping);
    }
}
