//! Service Lifecycle State Machine
//!
//! Provides the explicit state machine representing the lifecycle of hosted Rust services.
//! Implements strict transition validation, inspection methods, and error types.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Operational lifecycle state of a hosted service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceState {
    /// Instance created and configured, library not yet loaded.
    Created,
    /// Service dynamic library is being loaded and ABI validated.
    Loading,
    /// Service instance is initializing its runtime, threads, and dependencies.
    Starting,
    /// Service is healthy and actively executing.
    Running,
    /// A fault (panic, exception, health failure) was detected.
    /// The service is quarantined; automatic restart is prevented.
    FaultDetected,
    /// Active diagnostic session: thread stacks and telemetry being analyzed.
    Diagnosing,
    /// Service is undergoing controlled cooperative shutdown.
    Stopping,
    /// Service is completely stopped; all worker threads terminated.
    Stopped,
    /// Service is undergoing pre-flight verification prior to starting again.
    StartingAgain,
    /// Host process crashed, disconnected, or unrecoverable system failure.
    Unavailable,
}

impl ServiceState {
    /// Returns the human-readable display name of the state.
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceState::Created => "Created",
            ServiceState::Loading => "Loading",
            ServiceState::Starting => "Starting",
            ServiceState::Running => "Running",
            ServiceState::FaultDetected => "FaultDetected",
            ServiceState::Diagnosing => "Diagnosing",
            ServiceState::Stopping => "Stopping",
            ServiceState::Stopped => "Stopped",
            ServiceState::StartingAgain => "StartingAgain",
            ServiceState::Unavailable => "Unavailable",
        }
    }

    /// Evaluates whether transitioning from this state to the target state is valid.
    pub fn can_transition_to(&self, target: ServiceState) -> bool {
        if *self == target {
            return true;
        }

        match self {
            ServiceState::Created => matches!(
                target,
                ServiceState::Loading | ServiceState::Unavailable
            ),
            ServiceState::Loading => matches!(
                target,
                ServiceState::Starting | ServiceState::FaultDetected | ServiceState::Unavailable
            ),
            ServiceState::Starting => matches!(
                target,
                ServiceState::Running
                    | ServiceState::FaultDetected
                    | ServiceState::Stopping
                    | ServiceState::Unavailable
            ),
            ServiceState::Running => matches!(
                target,
                ServiceState::FaultDetected | ServiceState::Stopping | ServiceState::Unavailable
            ),
            ServiceState::FaultDetected => matches!(
                target,
                ServiceState::Diagnosing
                    | ServiceState::Stopping
                    | ServiceState::StartingAgain
                    | ServiceState::Running
                    | ServiceState::Unavailable
            ),
            ServiceState::Diagnosing => matches!(
                target,
                ServiceState::Stopping
                    | ServiceState::StartingAgain
                    | ServiceState::Running
                    | ServiceState::FaultDetected
                    | ServiceState::Unavailable
            ),
            ServiceState::Stopping => matches!(
                target,
                ServiceState::Stopped | ServiceState::FaultDetected | ServiceState::Unavailable
            ),
            ServiceState::Stopped => matches!(
                target,
                ServiceState::StartingAgain
                    | ServiceState::Starting
                    | ServiceState::Loading
                    | ServiceState::Created
                    | ServiceState::Unavailable
            ),
            ServiceState::StartingAgain => matches!(
                target,
                ServiceState::Starting
                    | ServiceState::FaultDetected
                    | ServiceState::Stopping
                    | ServiceState::Unavailable
            ),
            ServiceState::Unavailable => matches!(
                target,
                ServiceState::Created | ServiceState::Loading
            ),
        }
    }

    /// Attempts to transition the state in-place, returning an error if invalid.
    pub fn transition(&mut self, target: ServiceState) -> Result<(), StateTransitionError> {
        if self.can_transition_to(target) {
            *self = target;
            Ok(())
        } else {
            Err(StateTransitionError {
                from: *self,
                to: target,
            })
        }
    }

    /// Whether the service is currently executing (Running, Diagnosing, or FaultDetected).
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            ServiceState::Starting
                | ServiceState::Running
                | ServiceState::FaultDetected
                | ServiceState::Diagnosing
                | ServiceState::Stopping
                | ServiceState::StartingAgain
        )
    }

    /// Whether the service is in a faulted or diagnostic state.
    pub fn is_faulted(&self) -> bool {
        matches!(
            self,
            ServiceState::FaultDetected | ServiceState::Diagnosing
        )
    }

    /// Whether diagnostic inspection (stack capture, log retrieval) is allowed.
    pub fn allows_diagnostics(&self) -> bool {
        matches!(
            self,
            ServiceState::Running
                | ServiceState::FaultDetected
                | ServiceState::Diagnosing
                | ServiceState::Stopping
                | ServiceState::Stopped
        )
    }

    /// Whether the service is ready to accept user start command.
    pub fn can_start(&self) -> bool {
        matches!(self, ServiceState::Created | ServiceState::Stopped)
    }

    /// Whether the service is in a condition where stop command can be issued.
    pub fn can_stop(&self) -> bool {
        matches!(
            self,
            ServiceState::Starting
                | ServiceState::Running
                | ServiceState::FaultDetected
                | ServiceState::Diagnosing
                | ServiceState::StartingAgain
        )
    }

    /// Whether the service can be restarted.
    pub fn can_restart(&self) -> bool {
        matches!(
            self,
            ServiceState::Running
                | ServiceState::Stopped
                | ServiceState::FaultDetected
                | ServiceState::Diagnosing
        )
    }

    /// Suggested badge color for UI rendering (hex code).
    pub fn status_color_hex(&self) -> &'static str {
        match self {
            ServiceState::Running => "#10B981",       // Emerald Green
            ServiceState::Starting | ServiceState::StartingAgain | ServiceState::Loading => "#3B82F6", // Blue
            ServiceState::FaultDetected => "#EF4444",  // Red
            ServiceState::Diagnosing => "#F97316",     // Orange
            ServiceState::Stopping | ServiceState::Stopped => "#F59E0B", // Amber
            ServiceState::Created => "#8B5CF6",        // Purple
            ServiceState::Unavailable => "#6B7280",    // Muted Slate
        }
    }
}

impl fmt::Display for ServiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Error returned when attempting an illegal state transition.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[error("Invalid state transition from {from} to {to}")]
pub struct StateTransitionError {
    pub from: ServiceState,
    pub to: ServiceState,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_lifecycle_transitions() {
        let mut state = ServiceState::Created;
        assert!(state.transition(ServiceState::Loading).is_ok());
        assert!(state.transition(ServiceState::Starting).is_ok());
        assert!(state.transition(ServiceState::Running).is_ok());
        assert!(state.transition(ServiceState::Stopping).is_ok());
        assert!(state.transition(ServiceState::Stopped).is_ok());
        assert!(state.transition(ServiceState::StartingAgain).is_ok());
        assert!(state.transition(ServiceState::Starting).is_ok());
    }

    #[test]
    fn test_fault_quarantine_workflow() {
        let mut state = ServiceState::Running;
        // Fault is detected:
        assert!(state.transition(ServiceState::FaultDetected).is_ok());
        assert!(state.is_faulted());
        // Diagnostic session begins:
        assert!(state.transition(ServiceState::Diagnosing).is_ok());
        assert!(state.allows_diagnostics());
        // Operator decides to resume after fixing issue:
        let mut resume_state = state;
        assert!(resume_state.transition(ServiceState::Running).is_ok());

        // Alternatively, operator decides to stop:
        let mut stop_state = state;
        assert!(stop_state.transition(ServiceState::Stopping).is_ok());
        assert!(stop_state.transition(ServiceState::Stopped).is_ok());
    }

    #[test]
    fn test_invalid_transitions() {
        let mut state = ServiceState::Created;
        // Cannot jump directly from Created to Running:
        assert!(state.transition(ServiceState::Running).is_err());

        let mut stopped = ServiceState::Stopped;
        // Cannot jump directly from Stopped to Running without Starting:
        assert!(stopped.transition(ServiceState::Running).is_err());
    }

    #[test]
    fn test_idempotent_self_transitions() {
        let mut state = ServiceState::Running;
        assert!(state.transition(ServiceState::Running).is_ok());
    }
}
