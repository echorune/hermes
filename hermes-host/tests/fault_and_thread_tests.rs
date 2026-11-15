use chrono::Utc;
use hermes_core::models::{FaultSeverity, ThreadState};
use hermes_core::state::ServiceState;
use hermes_host::container::{ServiceContainer, ServiceInstance};
use hermes_host::thread_manager::{current_thread_id, ThreadManager};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn test_thread_manager_registration_and_tid() {
    let mgr = ThreadManager::new();
    let tid = current_thread_id();
    assert!(tid > 0);

    mgr.register_thread("svc-indexer", tid, "indexer-worker-1", true);

    let threads = mgr.get_service_threads("svc-indexer");
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].id, tid);
    assert_eq!(threads[0].name, "indexer-worker-1");
    assert_eq!(threads[0].state, ThreadState::Running);
}

#[test]
fn test_worker_panic_isolation_and_backtrace_capture() {
    let mgr = ThreadManager::new();
    let fault_notified = Arc::new(AtomicBool::new(false));
    let fault_notified_clone = fault_notified.clone();

    mgr.set_fault_callback(move |svc, tid, name, msg, stack| {
        assert_eq!(svc, "database-service");
        assert_eq!(name, "query-worker-3");
        assert!(msg.contains("Simulated database timeout"));
        assert!(tid > 0);
        assert!(!stack.frames.is_empty());
        fault_notified_clone.store(true, Ordering::SeqCst);
    });

    let handle = mgr
        .spawn_worker("database-service", "query-worker-3", || {
            // Deliberate panic inside worker thread
            panic!("Simulated database timeout / deadlock panic!");
        })
        .expect("Failed to spawn worker");

    // Joining the thread handle succeeds because catch_unwind handled the panic!
    let join_res = handle.join();
    assert!(join_res.is_ok());

    assert!(fault_notified.load(Ordering::SeqCst));

    // Thread is marked Faulted in the registry
    let threads = mgr.get_service_threads("database-service");
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].state, ThreadState::Faulted);
}

#[test]
fn test_container_fault_quarantine_and_cascading_containment() {
    let mut container = ServiceContainer::new("host-fault-test");

    // Parent service
    let parent = ServiceInstance {
        id: "order-pipeline".to_string(),
        name: "Order Pipeline".to_string(),
        plugin_path: "".to_string(),
        plugin: None,
        raw_instance: None,
        state: ServiceState::Stopped,
        parent: None,
        children: vec!["payment-worker".to_string()],
        config_json: "{}".to_string(),
        started_at: None,
        last_state_change: Utc::now(),
        last_fault: None,
        health_summary: "".to_string(),
    };

    // Dependent child service
    let child = ServiceInstance {
        id: "payment-worker".to_string(),
        name: "Payment Worker".to_string(),
        plugin_path: "".to_string(),
        plugin: None,
        raw_instance: None,
        state: ServiceState::Stopped,
        parent: Some("order-pipeline".to_string()),
        children: vec![],
        config_json: "{}".to_string(),
        started_at: None,
        last_state_change: Utc::now(),
        last_fault: None,
        health_summary: "".to_string(),
    };

    container.services.insert(parent.id.clone(), parent);
    container.services.insert(child.id.clone(), child);

    // Start both services
    let p_info = container.start_service("order-pipeline").unwrap();
    assert_eq!(p_info.state, ServiceState::Running);

    let c_info = container.start_service("payment-worker").unwrap();
    assert_eq!(c_info.state, ServiceState::Running);

    // Register active threads for parent
    container
        .thread_manager
        .register_thread("order-pipeline", 8881, "pipeline-worker-1", true);

    // Parent encounters a fault
    let fault_info = container
        .handle_service_fault(
            "order-pipeline",
            Some(8881),
            Some("pipeline-worker-1".to_string()),
            FaultSeverity::Fatal,
            "Critical state machine violation",
            serde_json::json!({ "queue_length": 50 }),
        )
        .unwrap();

    // 1. Parent is quarantined into FaultDetected
    assert_eq!(fault_info.state, ServiceState::FaultDetected);
    assert!(fault_info.last_fault.is_some());
    assert_eq!(
        fault_info.last_fault.as_ref().unwrap().error_message,
        "Critical state machine violation"
    );

    // 2. Cascading containment: Child service was automatically stopped/quiesced!
    let child_after = container.services.get("payment-worker").unwrap();
    assert_eq!(child_after.state, ServiceState::Stopped);

    // 3. Stacks are captured and queryable
    let stacks = container.get_thread_stacks("order-pipeline").unwrap();
    assert!(!stacks.is_empty());

    // 4. Operator investigates and clears fault
    let resumed_info = container.clear_fault("order-pipeline").unwrap();
    assert_eq!(resumed_info.state, ServiceState::Running);
    assert!(resumed_info.last_fault.is_none());
}

#[test]
fn test_procfs_thread_stack_inspection() {
    let mgr = ThreadManager::new();
    let current_tid = current_thread_id();

    let snapshot = mgr.capture_thread_stack(current_tid);
    assert_eq!(snapshot.thread_id, current_tid);
    assert!(!snapshot.frames.is_empty());
}
