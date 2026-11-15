//! Hermes Core Library
//!
//! Provides the architectural foundation for the Hermes Service Management Platform:
//! - C-compatible dynamic plugin ABI specification and safe wrapper macros (`abi`)
//! - Strict service lifecycle state machine engine (`state`)
//! - Framed JSON communication wire protocol and codecs (`protocol`)
//! - Strongly-typed domain models for hosts, services, threads, faults, and events (`models`)

pub mod abi;
pub mod models;
pub mod protocol;
pub mod state;

// Re-export core types for convenient usage
pub use abi::{
    c_char_to_rust_str, c_char_to_rust_string, free_c_string, rust_str_to_c_char,
    HermesPluginDescriptor, HermesPluginHeader, HermesPluginVTable, HermesServicePlugin,
    HERMES_ABI_VERSION, HERMES_PLUGIN_MAGIC,
};
pub use models::{
    CallStackFrame, CallStackSnapshot, FaultRecord, FaultSeverity, HostInfo, LogEntry, LogLevel,
    ServiceInfo, SystemEvent, SystemEventType, ThreadInfo, ThreadState,
};
pub use protocol::{
    decode_framed_message, encode_framed_message, ErrorCode, HermesMessage, ProtocolError,
    ProtocolErrorPayload, ServiceLifecycleCommand,
};
pub use state::{ServiceState, StateTransitionError};
