//! Host Configuration Management
//!
//! Parses and validates host TOML configuration files defining host identity,
//! root connection parameters, dynamic plugin libraries, and parent-child dependencies.

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

/// Host identity and connection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HostConfig {
    pub id: String,
    pub name: String,
    pub root_address: String,
    pub heartbeat_interval_secs: u64,
    pub log_level: String,
    pub log_dir: String,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            id: "host-default".to_string(),
            name: "Default Hermes Host".to_string(),
            root_address: "127.0.0.1:9090".to_string(),
            heartbeat_interval_secs: 2,
            log_level: "info".to_string(),
            log_dir: "logs".to_string(),
        }
    }
}

/// Configuration for an individual dynamically loaded service plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    pub id: String,
    pub name: String,
    pub path: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub parent: Option<String>,
    #[serde(default)]
    pub children: Vec<String>,
    #[serde(default)]
    pub config: serde_json::Value,
}

fn default_true() -> bool {
    true
}

/// Master host configuration file representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostFileConfig {
    #[serde(default)]
    pub host: HostConfig,
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
}

impl HostFileConfig {
    /// Loads configuration from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        let config: HostFileConfig = toml::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_config() {
        let toml_str = r#"
        [host]
        id = "host-alpha"
        name = "Alpha Host Process"
        root_address = "127.0.0.1:9090"
        heartbeat_interval_secs = 3

        [[plugins]]
        id = "order-processor"
        name = "Order Processing Pipeline"
        path = "target/debug/liborder_processor.so"
        enabled = true
        children = ["inventory-worker"]

        [plugins.config]
        batch_size = 50
        workers = 2

        [[plugins]]
        id = "inventory-worker"
        name = "Inventory Management Worker"
        path = "target/debug/libinventory_worker.so"
        parent = "order-processor"
        "#;

        let parsed: HostFileConfig = toml::from_str(toml_str).expect("Failed to parse TOML");
        assert_eq!(parsed.host.id, "host-alpha");
        assert_eq!(parsed.host.heartbeat_interval_secs, 3);
        assert_eq!(parsed.plugins.len(), 2);

        let parent = &parsed.plugins[0];
        assert_eq!(parent.id, "order-processor");
        assert_eq!(parent.children, vec!["inventory-worker".to_string()]);
        assert_eq!(parent.config["batch_size"], 50);

        let child = &parsed.plugins[1];
        assert_eq!(child.id, "inventory-worker");
        assert_eq!(child.parent.as_deref(), Some("order-processor"));
    }
}
