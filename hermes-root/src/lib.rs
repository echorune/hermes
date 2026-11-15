//! Hermes Root Management Service Library
//!
//! Exposes configuration, in-memory topology registry, command routing,
//! event logging, and the async TCP framed server engine.

pub mod config;
pub mod event_log;
pub mod registry;
pub mod router;
pub mod server;

pub use config::RootConfig;
pub use event_log::EventStore;
pub use registry::{HostRecord, HostStatus, Registry};
pub use router::CommandRouter;
pub use server::RootServer;
