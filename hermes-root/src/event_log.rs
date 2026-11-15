//! Operational Event & Log Store
//!
//! Maintains thread-safe in-memory ring buffers and optional JSONL file persistence
//! for audit events and streaming logs across the cluster.

use chrono::Utc;
use hermes_core::models::{LogEntry, LogLevel, SystemEvent, SystemEventType};
use std::collections::VecDeque;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::Path;
use tracing::warn;

pub struct EventStore {
    events: VecDeque<SystemEvent>,
    event_capacity: usize,
    logs: VecDeque<LogEntry>,
    log_capacity: usize,
    persist_file: Option<String>,
}

impl EventStore {
    pub fn new(event_capacity: usize, log_capacity: usize, persist_file: Option<String>) -> Self {
        if let Some(ref path_str) = persist_file {
            let path = Path::new(path_str);
            if let Some(parent) = path.parent() {
                let _ = create_dir_all(parent);
            }
        }

        Self {
            events: VecDeque::with_capacity(event_capacity),
            event_capacity,
            logs: VecDeque::with_capacity(log_capacity),
            log_capacity,
            persist_file,
        }
    }

    /// Records an audit event into memory and appends to disk if persistence is enabled.
    pub fn record_event(&mut self, event: SystemEvent) {
        if self.events.len() >= self.event_capacity {
            self.events.pop_front();
        }
        self.events.push_back(event.clone());

        if let Some(ref path) = self.persist_file {
            if let Ok(line) = serde_json::to_string(&event) {
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
                    let _ = writeln!(file, "{}", line);
                } else {
                    warn!("Failed to append event to audit file {}", path);
                }
            }
        }
    }

    /// Records a structured log entry into the log ring buffer.
    pub fn record_log(&mut self, log: LogEntry) {
        if self.logs.len() >= self.log_capacity {
            self.logs.pop_front();
        }
        self.logs.push_back(log);
    }

    /// Queries events in reverse chronological order with optional filters.
    pub fn query_events(
        &self,
        limit: usize,
        offset: usize,
        filter_host: Option<&str>,
        filter_service: Option<&str>,
    ) -> Vec<SystemEvent> {
        self.events
            .iter()
            .rev()
            .filter(|e| {
                if let Some(h) = filter_host {
                    if e.host_id != h {
                        return false;
                    }
                }
                if let Some(s) = filter_service {
                    if e.service_id.as_deref() != Some(s) {
                        return false;
                    }
                }
                true
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Queries logs in reverse chronological order with optional level, service, and text filters.
    pub fn query_logs(
        &self,
        limit: usize,
        min_level: Option<LogLevel>,
        filter_service: Option<&str>,
        search_text: Option<&str>,
    ) -> Vec<LogEntry> {
        let search_lower = search_text.map(|s| s.to_lowercase());

        self.logs
            .iter()
            .rev()
            .filter(|l| {
                if let Some(min) = min_level {
                    if l.level < min {
                        return false;
                    }
                }
                if let Some(s) = filter_service {
                    if l.service_id.as_deref() != Some(s) {
                        return false;
                    }
                }
                if let Some(ref query) = search_lower {
                    let msg_match = l.message.to_lowercase().contains(query);
                    let target_match = l.target.to_lowercase().contains(query);
                    if !msg_match && !target_match {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Helper to create and record a system event.
    pub fn emit_event(
        &mut self,
        event_type: SystemEventType,
        host_id: impl Into<String>,
        service_id: Option<String>,
        description: impl Into<String>,
        metadata: serde_json::Value,
    ) -> SystemEvent {
        let event = SystemEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            event_type,
            host_id: host_id.into(),
            service_id,
            description: description.into(),
            metadata,
        };
        self.record_event(event.clone());
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_ring_buffer_capacity() {
        let mut store = EventStore::new(3, 5, None);

        for i in 1..=5 {
            store.emit_event(
                SystemEventType::HostHeartbeat,
                format!("host-{}", i),
                None,
                format!("Heartbeat {}", i),
                serde_json::Value::Null,
            );
        }

        let events = store.query_events(10, 0, None, None);
        assert_eq!(events.len(), 3);
        // Reverse order (most recent first):
        assert_eq!(events[0].host_id, "host-5");
        assert_eq!(events[1].host_id, "host-4");
        assert_eq!(events[2].host_id, "host-3");
    }

    #[test]
    fn test_log_filtering() {
        let mut store = EventStore::new(10, 10, None);

        store.record_log(LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            host_id: "host-1".to_string(),
            service_id: Some("svc-a".to_string()),
            target: "module_a".to_string(),
            message: "Normal startup".to_string(),
        });

        store.record_log(LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Error,
            host_id: "host-1".to_string(),
            service_id: Some("svc-b".to_string()),
            target: "module_b".to_string(),
            message: "Database connection failed".to_string(),
        });

        // Filter by min level Warn (should only return Error)
        let filtered = store.query_logs(10, Some(LogLevel::Warn), None, None);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].level, LogLevel::Error);

        // Search text
        let searched = store.query_logs(10, None, None, Some("startup"));
        assert_eq!(searched.len(), 1);
        assert_eq!(searched[0].service_id.as_deref(), Some("svc-a"));
    }
}
