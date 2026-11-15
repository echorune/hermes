//! Service Container & Instance Lifecycle Manager
//!
//! Manages the lifecycle of dynamically loaded service libraries, enforces
//! explicit state transitions, orders parent-child startup and shutdown, and handles FFI memory safety.

use crate::config::PluginConfig;
use crate::fault_manager::FaultManager;
use crate::plugin_loader::{LoadedPlugin, PluginLoaderError};
use crate::thread_manager::ThreadManager;
use chrono::{DateTime, Utc};
use hermes_core::models::{CallStackSnapshot, FaultRecord, FaultSeverity, ServiceInfo, ThreadInfo};
use hermes_core::state::{ServiceState, StateTransitionError};
use std::collections::HashMap;
use std::ffi::c_void;
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum ContainerError {
    #[error("Plugin loader error for {plugin_id}: {source}")]
    PluginLoad {
        plugin_id: String,
        #[source]
        source: PluginLoaderError,
    },

    #[error("Service '{0}' not found in container")]
    ServiceNotFound(String),

    #[error("Invalid state transition for service '{service_id}': {source}")]
    InvalidTransition {
        service_id: String,
        #[source]
        source: StateTransitionError,
    },

    #[error("Dependency cycle detected involving service '{0}'")]
    DependencyCycle(String),

    #[error("Parent service '{parent}' for child '{child}' not found")]
    ParentNotFound { child: String, parent: String },

    #[error("JSON serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Managed runtime instance of an individual service.
pub struct ServiceInstance {
    pub id: String,
    pub name: String,
    pub plugin_path: String,
    pub plugin: Option<LoadedPlugin>,
    pub raw_instance: Option<*mut c_void>,
    pub state: ServiceState,
    pub parent: Option<String>,
    pub children: Vec<String>,
    pub config_json: String,
    pub started_at: Option<DateTime<Utc>>,
    pub last_state_change: DateTime<Utc>,
    pub last_fault: Option<FaultRecord>,
    pub health_summary: String,
}

// Safety: Access to ServiceInstance is protected by Container mutexes.
unsafe impl Send for ServiceInstance {}
unsafe impl Sync for ServiceInstance {}

impl Drop for ServiceInstance {
    fn drop(&mut self) {
        if let (Some(plugin), Some(raw)) = (&self.plugin, self.raw_instance) {
            let _ = plugin.stop_instance(raw);
            let _ = plugin.destroy_instance(raw);
        }
    }
}

impl ServiceInstance {
    pub fn to_service_info(&self, host_id: &str, thread_count: usize) -> ServiceInfo {
        let uptime_secs = self
            .started_at
            .map(|t| (Utc::now() - t).num_seconds().max(0) as u64)
            .unwrap_or(0);

        ServiceInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            host_id: host_id.to_string(),
            plugin_path: self.plugin_path.clone(),
            state: self.state,
            uptime_secs,
            thread_count,
            parent_service: self.parent.clone(),
            child_services: self.children.clone(),
            health_summary: self.health_summary.clone(),
            last_fault: self.last_fault.clone(),
            last_state_change: self.last_state_change,
        }
    }
}

pub struct ServiceContainer {
    pub host_id: String,
    pub services: HashMap<String, ServiceInstance>,
    pub library_cache: HashMap<String, LoadedPlugin>,
    pub thread_manager: ThreadManager,
    pub fault_manager: FaultManager,
}

impl ServiceContainer {
    pub fn new(host_id: impl Into<String>) -> Self {
        let host_id_str = host_id.into();
        let thread_mgr = ThreadManager::new();
        let fault_mgr = FaultManager::new(&host_id_str, None);

        Self {
            host_id: host_id_str,
            services: HashMap::new(),
            library_cache: HashMap::new(),
            thread_manager: thread_mgr,
            fault_manager: fault_mgr,
        }
    }

    /// Loads and instantiates all enabled plugins from configuration.
    pub fn initialize_plugins(
        &mut self,
        plugin_configs: &[PluginConfig],
    ) -> Result<(), ContainerError> {
        for conf in plugin_configs {
            if !conf.enabled {
                debug!(plugin_id = %conf.id, "Skipping disabled plugin");
                continue;
            }

            let path_key = conf.path.clone();
            // Cache loaded dynamic libraries to support multiple instances sharing the same library
            let loaded_plugin = if let Some(cached) = self.library_cache.get(&path_key) {
                cached.clone()
            } else {
                let p = LoadedPlugin::load(&conf.path).map_err(|e| ContainerError::PluginLoad {
                    plugin_id: conf.id.clone(),
                    source: e,
                })?;
                self.library_cache.insert(path_key.clone(), p.clone());
                p
            };

            let config_json = serde_json::to_string(&conf.config)?;
            let raw_instance = loaded_plugin
                .create_instance(&config_json)
                .map_err(|e| ContainerError::PluginLoad {
                    plugin_id: conf.id.clone(),
                    source: e,
                })?;

            let now = Utc::now();
            let instance = ServiceInstance {
                id: conf.id.clone(),
                name: conf.name.clone(),
                plugin_path: conf.path.clone(),
                plugin: Some(loaded_plugin),
                raw_instance: Some(raw_instance),
                state: ServiceState::Stopped,
                parent: conf.parent.clone(),
                children: conf.children.clone(),
                config_json,
                started_at: None,
                last_state_change: now,
                last_fault: None,
                health_summary: "Initialized".to_string(),
            };

            self.services.insert(conf.id.clone(), instance);
        }

        // Validate parent-child references
        self.validate_dependencies()?;

        Ok(())
    }

    /// Starts all configured services in dependency order (parents before children).
    pub fn start_all(&mut self) -> Result<Vec<ServiceInfo>, ContainerError> {
        let order = self.compute_startup_order()?;
        let mut results = Vec::new();

        for id in order {
            let info = self.start_service(&id)?;
            results.push(info);
        }

        Ok(results)
    }

    /// Stops all configured services in reverse dependency order (children before parents).
    pub fn stop_all(&mut self) -> Result<Vec<ServiceInfo>, ContainerError> {
        let mut order = self.compute_startup_order()?;
        order.reverse();
        let mut results = Vec::new();

        for id in order {
            if let Ok(info) = self.stop_service(&id) {
                results.push(info);
            }
        }

        Ok(results)
    }

    /// Starts a specific service instance.
    pub fn start_service(&mut self, id: &str) -> Result<ServiceInfo, ContainerError> {
        let svc = self
            .services
            .get_mut(id)
            .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;

        // State check & transition
        svc.state
            .transition(ServiceState::Starting)
            .map_err(|e| ContainerError::InvalidTransition {
                service_id: id.to_string(),
                source: e,
            })?;

        if let (Some(plugin), Some(raw)) = (&svc.plugin, svc.raw_instance) {
            plugin
                .start_instance(raw)
                .map_err(|e| ContainerError::PluginLoad {
                    plugin_id: id.to_string(),
                    source: e,
                })?;
        }

        svc.state
            .transition(ServiceState::Running)
            .map_err(|e| ContainerError::InvalidTransition {
                service_id: id.to_string(),
                source: e,
            })?;

        let now = Utc::now();
        svc.started_at = Some(now);
        svc.last_state_change = now;
        svc.health_summary = "Running Nominal".to_string();

        info!(service_id = %id, "Service started successfully");
        let thread_count = self.thread_manager.get_service_threads(id).len();
        Ok(svc.to_service_info(&self.host_id, thread_count))
    }

    /// Stops a specific service instance, stopping children first.
    pub fn stop_service(&mut self, id: &str) -> Result<ServiceInfo, ContainerError> {
        // Collect active children to stop them first
        let children_to_stop: Vec<String> = {
            let svc = self
                .services
                .get(id)
                .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;
            svc.children.clone()
        };

        for child_id in children_to_stop {
            if let Some(child) = self.services.get(&child_id) {
                if child.state.is_active() {
                    debug!(child = %child_id, parent = %id, "Stopping child before parent");
                    let _ = self.stop_service(&child_id);
                }
            }
        }

        let svc = self
            .services
            .get_mut(id)
            .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;

        svc.state
            .transition(ServiceState::Stopping)
            .map_err(|e| ContainerError::InvalidTransition {
                service_id: id.to_string(),
                source: e,
            })?;

        if let (Some(plugin), Some(raw)) = (&svc.plugin, svc.raw_instance) {
            let _ = plugin.stop_instance(raw);
        }

        svc.state
            .transition(ServiceState::Stopped)
            .map_err(|e| ContainerError::InvalidTransition {
                service_id: id.to_string(),
                source: e,
            })?;

        let now = Utc::now();
        svc.started_at = None;
        svc.last_state_change = now;
        svc.health_summary = "Stopped".to_string();

        info!(service_id = %id, "Service stopped successfully");
        let thread_count = self.thread_manager.get_service_threads(id).len();
        Ok(svc.to_service_info(&self.host_id, thread_count))
    }

    /// Restarts a service instance.
    pub fn restart_service(&mut self, id: &str) -> Result<ServiceInfo, ContainerError> {
        self.stop_service(id)?;
        self.start_service(id)
    }

    /// Transitions a faulted service back to Running if cleared by an operator.
    pub fn clear_fault(&mut self, id: &str) -> Result<ServiceInfo, ContainerError> {
        self.fault_manager.clear_active_fault(id);

        let svc = self
            .services
            .get_mut(id)
            .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;

        svc.state
            .transition(ServiceState::Running)
            .map_err(|e| ContainerError::InvalidTransition {
                service_id: id.to_string(),
                source: e,
            })?;

        svc.last_fault = None;
        svc.last_state_change = Utc::now();
        svc.health_summary = "Fault Cleared by Operator".to_string();
        let thread_count = self.thread_manager.get_service_threads(id).len();
        info!(service_id = %id, "Operator cleared service fault");
        Ok(svc.to_service_info(&self.host_id, thread_count))
    }

    /// Records a fault and quarantines the service in FaultDetected state.
    pub fn record_fault(&mut self, id: &str, fault: FaultRecord) -> Result<ServiceInfo, ContainerError> {
        let svc = self
            .services
            .get_mut(id)
            .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;

        svc.state = ServiceState::FaultDetected;
        svc.last_fault = Some(fault);
        svc.last_state_change = Utc::now();
        svc.health_summary = "Fault Detected - Quarantined".to_string();

        warn!(service_id = %id, "Service transitioned to FaultDetected quarantine");
        let thread_count = self.thread_manager.get_service_threads(id).len();
        Ok(svc.to_service_info(&self.host_id, thread_count))
    }

    /// Quarantines a service due to a fault, records diagnostic context,
    /// captures all thread stacks, and enforces cascading containment on child services.
    pub fn handle_service_fault(
        &mut self,
        service_id: &str,
        thread_id: Option<u64>,
        thread_name: Option<String>,
        severity: FaultSeverity,
        error_message: &str,
        context_data: serde_json::Value,
    ) -> Result<ServiceInfo, ContainerError> {
        let stacks = self.thread_manager.capture_all_service_stacks(service_id);

        let fault = self.fault_manager.record_fault(
            service_id,
            thread_id,
            thread_name,
            severity,
            error_message,
            stacks,
            context_data,
        );

        let svc_info = self.record_fault(service_id, fault)?;

        // Cascading containment: if this service has children, quiesce/stop them
        let children_to_quiesce = {
            if let Some(svc) = self.services.get(service_id) {
                svc.children.clone()
            } else {
                vec![]
            }
        };

        for child_id in children_to_quiesce {
            if let Some(child) = self.services.get(&child_id) {
                if child.state.is_active() {
                    warn!(
                        parent = %service_id,
                        child = %child_id,
                        "Containing cascading fault: quiescing dependent child service"
                    );
                    let _ = self.stop_service(&child_id);
                }
            }
        }

        Ok(svc_info)
    }

    /// Returns threads belonging to a service.
    pub fn get_service_threads(&self, id: &str) -> Vec<ThreadInfo> {
        self.thread_manager.get_service_threads(id)
    }

    /// Retrieves diagnostic JSON data from the service.
    pub fn get_diagnostics(&self, id: &str) -> Result<serde_json::Value, ContainerError> {
        let svc = self
            .services
            .get(id)
            .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;

        if let (Some(plugin), Some(raw)) = (&svc.plugin, svc.raw_instance) {
            let json_str = plugin
                .get_diagnostics_json(raw)
                .map_err(|e| ContainerError::PluginLoad {
                    plugin_id: id.to_string(),
                    source: e,
                })?;
            let parsed = serde_json::from_str(&json_str)?;
            Ok(parsed)
        } else {
            Ok(serde_json::json!({ "status": svc.state.as_str() }))
        }
    }

    /// Retrieves call stacks across all threads belonging to the service.
    pub fn get_thread_stacks(&self, id: &str) -> Result<Vec<CallStackSnapshot>, ContainerError> {
        let svc = self
            .services
            .get(id)
            .ok_or_else(|| ContainerError::ServiceNotFound(id.to_string()))?;

        // 1. Query thread manager stacks
        let mut manager_stacks = self.thread_manager.capture_all_service_stacks(id);

        // 2. Query plugin stacks if available
        if let (Some(plugin), Some(raw)) = (&svc.plugin, svc.raw_instance) {
            if let Ok(json_str) = plugin.get_thread_stacks_json(raw) {
                if let Ok(plugin_stacks) = serde_json::from_str::<Vec<CallStackSnapshot>>(&json_str) {
                    if !plugin_stacks.is_empty() {
                        manager_stacks.extend(plugin_stacks);
                    }
                }
            }
        }

        // 3. If empty, capture current thread stack as baseline
        if manager_stacks.is_empty() {
            let self_tid = crate::thread_manager::current_thread_id();
            manager_stacks.push(self.thread_manager.capture_thread_stack(self_tid));
        }

        Ok(manager_stacks)
    }

    /// Returns complete snapshot of all hosted service infos.
    pub fn get_topology(&self) -> Vec<ServiceInfo> {
        self.services
            .values()
            .map(|s| {
                let thread_count = self.thread_manager.get_service_threads(&s.id).len();
                s.to_service_info(&self.host_id, thread_count)
            })
            .collect()
    }

    /// Validates that parent-child relationship definitions are consistent.
    fn validate_dependencies(&self) -> Result<(), ContainerError> {
        for (id, svc) in &self.services {
            if let Some(ref parent_id) = svc.parent {
                if !self.services.contains_key(parent_id) {
                    return Err(ContainerError::ParentNotFound {
                        child: id.clone(),
                        parent: parent_id.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Computes topological startup order (parents before children).
    fn compute_startup_order(&self) -> Result<Vec<String>, ContainerError> {
        let mut order = Vec::new();
        let mut visited = HashMap::new(); // false = in progress, true = finished

        fn visit(
            id: &str,
            services: &HashMap<String, ServiceInstance>,
            visited: &mut HashMap<String, bool>,
            order: &mut Vec<String>,
        ) -> Result<(), ContainerError> {
            if let Some(&done) = visited.get(id) {
                if !done {
                    return Err(ContainerError::DependencyCycle(id.to_string()));
                }
                return Ok(());
            }

            visited.insert(id.to_string(), false);

            if let Some(svc) = services.get(id) {
                // If this service has a parent, visit the parent first
                if let Some(ref parent_id) = svc.parent {
                    if services.contains_key(parent_id) {
                        visit(parent_id, services, visited, order)?;
                    }
                }
            }

            visited.insert(id.to_string(), true);
            if !order.contains(&id.to_string()) {
                order.push(id.to_string());
            }
            Ok(())
        }

        for id in self.services.keys() {
            visit(id, &self.services, &mut visited, &mut order)?;
        }

        Ok(order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topological_dependency_ordering() {
        let mut container = ServiceContainer::new("host-test");

        // Insert Child
        let child = ServiceInstance {
            id: "inventory-worker".to_string(),
            name: "Child".to_string(),
            plugin_path: "".to_string(),
            plugin: None,
            raw_instance: None,
            state: ServiceState::Stopped,
            parent: Some("order-processor".to_string()),
            children: vec![],
            config_json: "{}".to_string(),
            started_at: None,
            last_state_change: Utc::now(),
            last_fault: None,
            health_summary: "".to_string(),
        };

        // Insert Parent
        let parent = ServiceInstance {
            id: "order-processor".to_string(),
            name: "Parent".to_string(),
            plugin_path: "".to_string(),
            plugin: None,
            raw_instance: None,
            state: ServiceState::Stopped,
            parent: None,
            children: vec!["inventory-worker".to_string()],
            config_json: "{}".to_string(),
            started_at: None,
            last_state_change: Utc::now(),
            last_fault: None,
            health_summary: "".to_string(),
        };

        container.services.insert(child.id.clone(), child);
        container.services.insert(parent.id.clone(), parent);

        let order = container.compute_startup_order().unwrap();
        assert_eq!(order.len(), 2);
        // Parent must come before child:
        assert_eq!(order[0], "order-processor");
        assert_eq!(order[1], "inventory-worker");
    }

    #[test]
    fn test_service_lifecycle_transitions() {
        let mut container = ServiceContainer::new("host-test");

        let svc = ServiceInstance {
            id: "telemetry".to_string(),
            name: "Telemetry".to_string(),
            plugin_path: "".to_string(),
            plugin: None,
            raw_instance: None,
            state: ServiceState::Stopped,
            parent: None,
            children: vec![],
            config_json: "{}".to_string(),
            started_at: None,
            last_state_change: Utc::now(),
            last_fault: None,
            health_summary: "".to_string(),
        };
        container.services.insert(svc.id.clone(), svc);

        // Start service
        let info = container.start_service("telemetry").unwrap();
        assert_eq!(info.state, ServiceState::Running);

        // Stop service
        let info_stopped = container.stop_service("telemetry").unwrap();
        assert_eq!(info_stopped.state, ServiceState::Stopped);
    }
}
