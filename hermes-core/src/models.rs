//! Shared Domain Models for Hermes Platform
//!
//! Strongly-typed domain representations of hosts, services, threads,
//! diagnostic snapshots, call stacks, fault records, and cluster events.

use crate::state::ServiceState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Operational metadata and status of an active Hermes Host instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostInfo {
    pub id: String,
    pub name: String,
    pub hostname: String,
    pub pid: u32,
    pub os: String,
    pub uptime_secs: u64,
    pub last_heartbeat: DateTime<Utc>,
    pub services: Vec<ServiceInfo>,
}

/// Operational metadata, health, and relationship status of an individual hosted service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub id: String,
    pub name: String,
    pub host_id: String,
    pub plugin_path: String,
    pub state: ServiceState,
    pub uptime_secs: u64,
    pub thread_count: usize,
    pub parent_service: Option<String>,
    pub child_services: Vec<String>,
    pub health_summary: String,
    pub last_fault: Option<FaultRecord>,
    pub last_state_change: DateTime<Utc>,
}

/// Operating state of an individual thread belonging to a service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadState {
    Running,
    Sleeping,
    Blocked,
    Faulted,
    Terminated,
}

impl ThreadState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ThreadState::Running => "Running",
            ThreadState::Sleeping => "Sleeping",
            ThreadState::Blocked => "Blocked",
            ThreadState::Faulted => "Faulted",
            ThreadState::Terminated => "Terminated",
        }
    }
}

/// Information regarding a monitored thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadInfo {
    pub id: u64,
    pub name: String,
    pub state: ThreadState,
    pub cpu_usage_pct: f32,
    pub is_worker: bool,
    pub last_seen: DateTime<Utc>,
}

/// An individual stack frame inside a call stack backtrace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallStackFrame {
    pub index: usize,
    pub symbol: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub col: Option<u32>,
}

/// Captured call stack snapshot for a specific thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallStackSnapshot {
    pub captured_at: DateTime<Utc>,
    pub thread_id: u64,
    pub thread_name: String,
    pub frames: Vec<CallStackFrame>,
    pub raw_backtrace: Option<String>,
}

/// Severity classification of an operational fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultSeverity {
    Warning,
    Degraded,
    Fatal,
}

/// Record of an intercepted fault, panic, or operational failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultRecord {
    pub fault_id: String,
    pub service_id: String,
    pub host_id: String,
    pub thread_id: Option<u64>,
    pub thread_name: Option<String>,
    pub severity: FaultSeverity,
    pub error_message: String,
    pub timestamp: DateTime<Utc>,
    pub call_stacks: Vec<CallStackSnapshot>,
    pub context_data: serde_json::Value,
}

/// Log severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

/// Structured log entry emitted by host or service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub host_id: String,
    pub service_id: Option<String>,
    pub target: String,
    pub message: String,
}

/// Categorical types of operational events across the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemEventType {
    HostRegistered,
    HostDisconnected,
    HostHeartbeat,
    ServiceLoaded,
    ServiceStarted,
    ServiceStopped,
    ServiceRestarted,
    FaultDetected,
    DiagnosticCapture,
    LogBackupInitiated,
    LogBackupCompleted,
    OperatorIntervention,
}

/// Audit event recording an infrastructure action or state change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemEvent {
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub event_type: SystemEventType,
    pub host_id: String,
    pub service_id: Option<String>,
    pub description: String,
    pub metadata: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_info_serialization() {
        let host = HostInfo {
            id: "host-1".to_string(),
            name: "Alpha Host".to_string(),
            hostname: "alpha.local".to_string(),
            pid: 12345,
            os: "linux".to_string(),
            uptime_secs: 4200,
            last_heartbeat: Utc::now(),
            services: vec![ServiceInfo {
                id: "svc-1".to_string(),
                name: "Telemetry".to_string(),
                host_id: "host-1".to_string(),
                plugin_path: "/lib/libtelemetry.so".to_string(),
                state: ServiceState::Running,
                uptime_secs: 120,
                thread_count: 3,
                parent_service: None,
                child_services: vec![],
                health_summary: "Nominal".to_string(),
                last_fault: None,
                last_state_change: Utc::now(),
            }],
        };

        let json = serde_json::to_string_pretty(&host).expect("Serialization failed");
        let decoded: HostInfo = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(decoded.id, host.id);
        assert_eq!(decoded.services.len(), 1);
        assert_eq!(decoded.services[0].state, ServiceState::Running);
    }

    #[test]
    fn test_fault_record_serialization() {
        let fault = FaultRecord {
            fault_id: "fault-99".to_string(),
            service_id: "order-proc".to_string(),
            host_id: "host-1".to_string(),
            thread_id: Some(42),
            thread_name: Some("worker-2".to_string()),
            severity: FaultSeverity::Fatal,
            error_message: "Division by zero in worker thread".to_string(),
            timestamp: Utc::now(),
            call_stacks: vec![CallStackSnapshot {
                captured_at: Utc::now(),
                thread_id: 42,
                thread_name: "worker-2".to_string(),
                frames: vec![CallStackFrame {
                    index: 0,
                    symbol: "order_processor::worker::calculate".to_string(),
                    file: Some("src/worker.rs".to_string()),
                    line: Some(88),
                    col: Some(12),
                }],
                raw_backtrace: None,
            }],
            context_data: serde_json::json!({ "queue_depth": 14 }),
        };

        let json = serde_json::to_string(&fault).unwrap();
        let decoded: FaultRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.fault_id, "fault-99");
        assert_eq!(decoded.call_stacks.len(), 1);
        assert_eq!(decoded.call_stacks[0].frames[0].line, Some(88));
    }
}
