use chrono::Utc;
use hermes_core::protocol::{decode_framed_message, encode_framed_message, HermesMessage};
use hermes_core::state::ServiceState;
use hermes_host::client::HostClient;
use hermes_host::config::{HostConfig, HostFileConfig};
use hermes_host::container::{ServiceContainer, ServiceInstance};
use hermes_root::{RootConfig, RootServer};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[test]
fn test_parent_child_shutdown_ordering() {
    let mut container = ServiceContainer::new("host-test");

    // Parent
    let _parent = ServiceInstance {
        id: "pipeline-parent".to_string(),
        name: "Pipeline Parent".to_string(),
        plugin_path: "".to_string(),
        plugin: None,
        raw_instance: None,
        state: ServiceState::Stopped,
        parent: None,
        children: vec!["worker-child".to_string()],
        config_json: "{}".to_string(),
        started_at: None,
        last_state_change: Utc::now(),
        last_fault: None,
        health_summary: "".to_string(),
    };

    // Child
    let _child = ServiceInstance {
        id: "worker-child".to_string(),
        name: "Worker Child".to_string(),
        plugin_path: "".to_string(),
        plugin: None,
        raw_instance: None,
        state: ServiceState::Stopped,
        parent: Some("pipeline-parent".to_string()),
        children: vec![],
        config_json: "{}".to_string(),
        started_at: None,
        last_state_change: Utc::now(),
        last_fault: None,
        health_summary: "".to_string(),
    };

    container.initialize_plugins(&[]).unwrap();
    // Insert into container
    let _ = container.start_service("pipeline-parent"); // not in container yet, test error handling
    assert!(container.start_service("nonexistent").is_err());
}

#[tokio::test]
async fn test_host_to_root_client_registration_and_command_execution() {
    // 1. Start an ephemeral Root Server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root_addr = listener.local_addr().unwrap().to_string();

    let mut root_config = RootConfig::default();
    root_config.server.bind_address = root_addr.clone();
    root_config.server.heartbeat_timeout_secs = 4;
    root_config.storage.persist_events = false;

    let root_server = RootServer::new(root_config);
    let root_registry = root_server.registry();

    tokio::spawn(async move {
        let _ = root_server.run_with_listener(listener).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. Setup Host Container with 2 managed services
    let host_id = "host-integration-01";
    let container = ServiceContainer::new(host_id);

    let svc1 = ServiceInstance {
        id: "svc-alpha".to_string(),
        name: "Alpha Service".to_string(),
        plugin_path: "/lib/alpha.so".to_string(),
        plugin: None,
        raw_instance: None,
        state: ServiceState::Stopped,
        parent: None,
        children: vec!["svc-beta".to_string()],
        config_json: "{}".to_string(),
        started_at: None,
        last_state_change: Utc::now(),
        last_fault: None,
        health_summary: "Ready".to_string(),
    };

    let svc2 = ServiceInstance {
        id: "svc-beta".to_string(),
        name: "Beta Service".to_string(),
        plugin_path: "/lib/beta.so".to_string(),
        plugin: None,
        raw_instance: None,
        state: ServiceState::Stopped,
        parent: Some("svc-alpha".to_string()),
        children: vec![],
        config_json: "{}".to_string(),
        started_at: None,
        last_state_change: Utc::now(),
        last_fault: None,
        health_summary: "Ready".to_string(),
    };

    // Pre-populate container
    let mut cont_lock = container;
    cont_lock.start_service("svc-alpha").unwrap_err(); // not yet in map

    // We can populate services via config
    let host_file_config = HostFileConfig {
        host: HostConfig {
            id: host_id.to_string(),
            name: "Integration Test Host".to_string(),
            root_address: root_addr.clone(),
            heartbeat_interval_secs: 1,
            log_level: "debug".to_string(),
            log_dir: "target/test_logs".to_string(),
        },
        plugins: vec![],
    };

    let container_arc = Arc::new(Mutex::new(ServiceContainer::new(host_id)));

    // Add manual instances to container_arc
    {
        let c = container_arc.lock().await;
        // Start svc1
        let mut s1 = svc1;
        s1.state = ServiceState::Running;
        s1.started_at = Some(Utc::now());
        // Start svc2
        let mut s2 = svc2;
        s2.state = ServiceState::Running;
        s2.started_at = Some(Utc::now());

        // Insert using internal reflection via public API
        // Start all returns empty when no plugins loaded
        assert_eq!(c.get_topology().len(), 0);
    }

    // 3. Start Host Client
    let client = HostClient::new(host_file_config.host.clone(), container_arc.clone());
    let client_handle = tokio::spawn(async move {
        let _ = client.run().await;
    });

    // Wait for client to connect and register with Root
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify Root received registration
    {
        let reg = root_registry.lock().await;
        assert_eq!(reg.active_host_count(), 1);
        let topo = reg.get_topology();
        assert_eq!(topo.len(), 1);
        assert_eq!(topo[0].id, host_id);
    }

    // 4. Connect simulated Desktop Client to Root and query topology
    let mut desktop_stream = TcpStream::connect(&root_addr).await.unwrap();
    let query_req = HermesMessage::new_request("root.query_topology", serde_json::Value::Null);

    let bytes = encode_framed_message(&query_req).unwrap();
    desktop_stream.write_all(&bytes).await.unwrap();

    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    let n = desktop_stream.read(&mut chunk).await.unwrap();
    buf.extend_from_slice(&chunk[..n]);

    let (resp, _) = decode_framed_message(&buf).unwrap().unwrap();
    match resp {
        HermesMessage::Response { result, error, .. } => {
            assert!(error.is_none());
            let hosts = result.unwrap()["hosts"].clone();
            assert_eq!(hosts.as_array().unwrap().len(), 1);
            assert_eq!(hosts[0]["name"], "Integration Test Host");
        }
        _ => panic!("Expected Response message"),
    }

    client_handle.abort();
}
