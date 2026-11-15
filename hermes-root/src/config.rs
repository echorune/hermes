//! Root Service Configuration Management
//!
//! Loads and validates root server binding, heartbeat timeouts, and storage parameters.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read configuration file: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse TOML configuration: {0}")]
    Toml(#[from] toml::de::Error),
}

/// Server network and heartbeat settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// IP and port to bind TCP listener.
    pub bind_address: String,
    /// Seconds without a heartbeat before marking host as unavailable.
    pub heartbeat_timeout_secs: u64,
    /// Frequency of background stale host checks.
    pub heartbeat_check_interval_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:9090".to_string(),
            heartbeat_timeout_secs: 6,
            heartbeat_check_interval_secs: 2,
        }
    }
}

/// Storage, persistence, and ring buffer capacities.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Maximum number of audit events retained in memory ring buffer.
    pub event_log_capacity: usize,
    /// Maximum number of recent log entries retained in memory.
    pub log_history_capacity: usize,
    /// Whether to append events to file.
    pub persist_events: bool,
    /// File path for JSONL event audit persistence.
    pub events_file: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            event_log_capacity: 2000,
            log_history_capacity: 5000,
            persist_events: true,
            events_file: "logs/root_events.jsonl".to_string(),
        }
    }
}

/// Master Root daemon configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RootConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

impl RootConfig {
    /// Loads configuration from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let config: RootConfig = toml::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RootConfig::default();
        assert_eq!(config.server.bind_address, "127.0.0.1:9090");
        assert_eq!(config.server.heartbeat_timeout_secs, 6);
        assert_eq!(config.storage.event_log_capacity, 2000);
    }

    #[test]
    fn test_parse_toml_config() {
        let toml_str = r#"
        [server]
        bind_address = "0.0.0.0:9999"
        heartbeat_timeout_secs = 10
        heartbeat_check_interval_secs = 3

        [storage]
        event_log_capacity = 500
        persist_events = false
        "#;

        let config: RootConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.bind_address, "0.0.0.0:9999");
        assert_eq!(config.server.heartbeat_timeout_secs, 10);
        assert_eq!(config.storage.event_log_capacity, 500);
        assert!(!config.storage.persist_events);
    }
}
