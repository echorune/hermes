//! Hermes Service Host Library
//!
//! Provides the runtime container, dynamic plugin loader, TOML configuration engine,
//! and host-to-root communication client.

pub mod client;
pub mod config;
pub mod container;
pub mod fault_manager;
pub mod plugin_loader;
pub mod thread_manager;

pub use client::HostClient;
pub use config::{HostConfig, HostFileConfig, PluginConfig};
pub use container::{ContainerError, ServiceContainer, ServiceInstance};
pub use fault_manager::FaultManager;
pub use plugin_loader::{LoadedPlugin, PluginLoaderError};
pub use thread_manager::ThreadManager;
