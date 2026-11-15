use chrono::Utc;
use hermes_core::abi::{c_char_to_rust_string, free_c_string, rust_str_to_c_char};
use hermes_core::models::{LogEntry, LogLevel, SystemEvent, SystemEventType};
use hermes_core::protocol::{
    decode_framed_message, encode_framed_message, ErrorCode, HermesMessage,
    ServiceLifecycleCommand,
};
use hermes_core::state::{ServiceState, StateTransitionError};

#[test]
fn test_end_to_end_state_machine_with_fault_quarantine() {
    let mut state = ServiceState::Created;
    assert_eq!(state.as_str(), "Created");

    // Start sequence
    state.transition(ServiceState::Loading).unwrap();
    state.transition(ServiceState::Starting).unwrap();
    state.transition(ServiceState::Running).unwrap();
    assert!(state.is_active());
    assert!(!state.is_faulted());

    // Fault detected (e.g. panic in worker thread)
    state.transition(ServiceState::FaultDetected).unwrap();
    assert!(state.is_faulted());
    assert!(state.allows_diagnostics());

    // Auto-restart bypassing recovery is strictly blocked by state machine invariants
    // Cannot jump directly from FaultDetected to Starting or Created:
    assert_eq!(
        state.transition(ServiceState::Starting).unwrap_err(),
        StateTransitionError {
            from: ServiceState::FaultDetected,
            to: ServiceState::Starting,
        }
    );
    assert_eq!(
        state.transition(ServiceState::Created).unwrap_err(),
        StateTransitionError {
            from: ServiceState::FaultDetected,
            to: ServiceState::Created,
        }
    );

    // Operator enters diagnostic investigation session
    state.transition(ServiceState::Diagnosing).unwrap();
    assert!(state.allows_diagnostics());

    // Operator initiates log backup, then commands controlled quiescence/stop
    state.transition(ServiceState::Stopping).unwrap();
    assert_eq!(state.as_str(), "Stopping");

    // All threads joined
    state.transition(ServiceState::Stopped).unwrap();
    assert!(!state.is_active());
    assert!(state.can_start());

    // Operator chooses to restart service
    state.transition(ServiceState::StartingAgain).unwrap();
    state.transition(ServiceState::Starting).unwrap();
    state.transition(ServiceState::Running).unwrap();
    assert_eq!(state, ServiceState::Running);
}

#[test]
fn test_all_message_variants_wire_encoding() {
    let messages = vec![
        HermesMessage::new_request(
            "service.command",
            serde_json::json!({
                "host_id": "host-us-east-1",
                "service_id": "telemetry",
                "command": ServiceLifecycleCommand::Restart,
            }),
        ),
        HermesMessage::new_response(
            "req-42",
            serde_json::json!({ "status": "acknowledged", "pid": 9876 }),
        ),
        HermesMessage::new_error_response(
            "req-43",
            ErrorCode::ServiceNotFound,
            "Service 'payment-gateway' not found on host 'host-us-east-1'",
        ),
        HermesMessage::Heartbeat {
            host_id: "host-us-east-1".to_string(),
            timestamp: Utc::now(),
        },
        HermesMessage::Event(SystemEvent {
            event_id: "evt-001".to_string(),
            timestamp: Utc::now(),
            event_type: SystemEventType::FaultDetected,
            host_id: "host-us-east-1".to_string(),
            service_id: Some("telemetry".to_string()),
            description: "Thread panic: index out of bounds".to_string(),
            metadata: serde_json::json!({ "thread_id": 104 }),
        }),
        HermesMessage::Log(LogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Warn,
            host_id: "host-us-east-1".to_string(),
            service_id: Some("telemetry".to_string()),
            target: "telemetry::collector".to_string(),
            message: "Buffer high-water mark reached: 85%".to_string(),
        }),
    ];

    for original in messages {
        let encoded = encode_framed_message(&original).expect("Encoding failed");
        let (decoded, consumed) = decode_framed_message(&encoded)
            .expect("Decoding failed")
            .expect("Incomplete frame");
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }
}

#[test]
fn test_abi_safe_string_helpers() {
    let original = "Hermes Dynamic Service ABI String \u{1F980}";
    let c_ptr = rust_str_to_c_char(original);
    assert!(!c_ptr.is_null());

    let recovered = unsafe { c_char_to_rust_string(c_ptr).expect("Failed to convert") };
    assert_eq!(recovered, original);

    unsafe {
        free_c_string(c_ptr);
    }
}
