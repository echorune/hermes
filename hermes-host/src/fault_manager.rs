//! Fault Management, Panic Interception & Diagnostic Quarantine Engine
//!
//! Intercepts process-wide and thread-level panics, preserves diagnostic evidence,
//! enforces strict quarantine invariants, and manages parent-child fault containment.

use chrono::Utc;
use hermes_core::models::{CallStackSnapshot, FaultRecord, FaultSeverity};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct FaultManager {
    host_id: String,
    active_faults: Arc<RwLock<HashMap<String, FaultRecord>>>,
    fault_history: Arc<RwLock<HashMap<String, Vec<FaultRecord>>>>,
    fault_notifier: Option<mpsc::Sender<FaultRecord>>,
}

impl FaultManager {
    pub fn new(host_id: impl Into<String>, fault_notifier: Option<mpsc::Sender<FaultRecord>>) -> Self {
        Self {
            host_id: host_id.into(),
            active_faults: Arc::new(RwLock::new(HashMap::new())),
            fault_history: Arc::new(RwLock::new(HashMap::new())),
            fault_notifier,
        }
    }

    /// Records an intercepted fault and preserves its call stacks and operational context.
    pub fn record_fault(
        &self,
        service_id: &str,
        thread_id: Option<u64>,
        thread_name: Option<String>,
        severity: FaultSeverity,
        error_message: impl Into<String>,
        call_stacks: Vec<CallStackSnapshot>,
        context_data: serde_json::Value,
    ) -> FaultRecord {
        let fault_id = format!("fault-{}", uuid::Uuid::new_v4());
        let error_msg = error_message.into();

        warn!(
            host_id = %self.host_id,
            service_id = %service_id,
            fault_id = %fault_id,
            thread = ?thread_name,
            error = %error_msg,
            "Quarantining service: fault recorded"
        );

        let fault = FaultRecord {
            fault_id,
            service_id: service_id.to_string(),
            host_id: self.host_id.clone(),
            thread_id,
            thread_name,
            severity,
            error_message: error_msg,
            timestamp: Utc::now(),
            call_stacks,
            context_data,
        };

        // Store in active faults
        {
            let mut active = self.active_faults.write().unwrap();
            active.insert(service_id.to_string(), fault.clone());
        }

        // Append to history
        {
            let mut history = self.fault_history.write().unwrap();
            history
                .entry(service_id.to_string())
                .or_insert_with(Vec::new)
                .push(fault.clone());
        }

        // Notify client sender if attached
        if let Some(ref tx) = self.fault_notifier {
            let _ = tx.try_send(fault.clone());
        }

        fault
    }

    /// Retrieves the current active fault for a service.
    pub fn get_active_fault(&self, service_id: &str) -> Option<FaultRecord> {
        let active = self.active_faults.read().unwrap();
        active.get(service_id).cloned()
    }

    /// Clears the active fault when an operator explicitly resolves or restarts the service.
    pub fn clear_active_fault(&self, service_id: &str) -> Option<FaultRecord> {
        let mut active = self.active_faults.write().unwrap();
        let removed = active.remove(service_id);
        if let Some(ref f) = removed {
            info!(
                service_id = %service_id,
                fault_id = %f.fault_id,
                "Operator cleared active service fault"
            );
        }
        removed
    }

    /// Returns historical fault records for a service.
    pub fn get_fault_history(&self, service_id: &str) -> Vec<FaultRecord> {
        let history = self.fault_history.read().unwrap();
        history.get(service_id).cloned().unwrap_or_default()
    }
}

/// Installs a global panic hook that captures panic details without aborting the host process.
pub fn install_process_panic_hook(
    host_id: String,
    fault_sender: mpsc::Sender<FaultRecord>,
) {
    let _default_hook = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |panic_info| {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown-thread").to_string();
        let tid = crate::thread_manager::current_thread_id();

        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic payload".to_string()
        };

        let location = panic_info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown location".to_string());

        let backtrace = std::backtrace::Backtrace::capture();
        let raw_trace = format!("{:#?}", backtrace);

        error!(
            host_id = %host_id,
            tid = tid,
            thread = %thread_name,
            location = %location,
            panic = %message,
            "Global panic hook intercepted thread crash! Quarantining failure."
        );

        let fault = FaultRecord {
            fault_id: format!("panic-{}", uuid::Uuid::new_v4()),
            service_id: "host-internal".to_string(),
            host_id: host_id.clone(),
            thread_id: Some(tid),
            thread_name: Some(thread_name),
            severity: FaultSeverity::Fatal,
            error_message: format!("Panic at {}: {}", location, message),
            timestamp: Utc::now(),
            call_stacks: vec![CallStackSnapshot {
                captured_at: Utc::now(),
                thread_id: tid,
                thread_name: "panicking-thread".to_string(),
                frames: vec![],
                raw_backtrace: Some(raw_trace),
            }],
            context_data: serde_json::json!({
                "location": location,
                "panic_type": "rust_panic"
            }),
        };

        let _ = fault_sender.try_send(fault);

        // Do NOT call default_hook unless explicitly configured, to prevent immediate abort.
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_clear_fault() {
        let manager = FaultManager::new("host-1", None);

        let fault = manager.record_fault(
            "order-proc",
            Some(42),
            Some("worker-1".to_string()),
            FaultSeverity::Fatal,
            "Division by zero in processing batch",
            vec![],
            serde_json::json!({ "batch_id": 999 }),
        );

        assert_eq!(fault.service_id, "order-proc");
        assert_eq!(fault.thread_id, Some(42));

        // Active fault exists
        let active = manager.get_active_fault("order-proc");
        assert!(active.is_some());
        assert_eq!(active.unwrap().fault_id, fault.fault_id);

        // Clear fault
        let cleared = manager.clear_active_fault("order-proc");
        assert!(cleared.is_some());
        assert!(manager.get_active_fault("order-proc").is_none());

        // History retained
        let history = manager.get_fault_history("order-proc");
        assert_eq!(history.len(), 1);
    }
}
