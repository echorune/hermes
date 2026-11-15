use chrono::Utc;
use hermes_core::models::{HostInfo, LogEntry, LogLevel, ServiceInfo};
use hermes_core::protocol::{
    decode_framed_message, encode_framed_message, ErrorCode, HermesMessage,
    RegisterHostParams, ServiceLifecycleCommand,
};
use hermes_core::state::ServiceState;
use hermes_root::{RootConfig, RootServer};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_test_server() -> (String, tokio::task::JoinHandle<()>) {
    // Bind to an OS-assigned ephemeral port (127.0.0.1:0)
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let mut config = RootConfig::default();
    config.server.bind_address = addr.clone();
    config.server.heartbeat_timeout_secs = 2;
    config.server.heartbeat_check_interval_secs = 1;
    config.storage.persist_events = false;

    let server = RootServer::new(config);

    let handle = tokio::spawn(async move {
        let _ = server.run_with_listener(listener).await;
    });

    // Give server moment to initialize
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

async fn read_framed_message(stream: &mut TcpStream) -> HermesMessage {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            panic!("Unexpected EOF while reading framed message");
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some((msg, _)) = decode_framed_message(&buffer).unwrap() {
            return msg;
        }
    }
}

async fn write_framed_message(stream: &mut TcpStream, msg: &HermesMessage) {
    let bytes = encode_framed_message(msg).unwrap();
    stream.write_all(&bytes).await.unwrap();
}

#[tokio::test]
async fn test_root_host_registration_and_client_topology_query() {
    let (addr, _server_handle) = start_test_server().await;

    // 1. Connect a simulated Service Host
    let mut host_stream = TcpStream::connect(&addr).await.unwrap();

    let host_info = HostInfo {
        id: "host-test-1".to_string(),
        name: "Test Host 1".to_string(),
        hostname: "node-1.cluster".to_string(),
        pid: 2468,
        os: "linux".to_string(),
        uptime_secs: 50,
        last_heartbeat: Utc::now(),
        services: vec![ServiceInfo {
            id: "telemetry".to_string(),
            name: "Telemetry Service".to_string(),
            host_id: "host-test-1".to_string(),
            plugin_path: "/opt/hermes/telemetry.so".to_string(),
            state: ServiceState::Running,
            uptime_secs: 50,
            thread_count: 4,
            parent_service: None,
            child_services: vec![],
            health_summary: "Healthy".to_string(),
            last_fault: None,
            last_state_change: Utc::now(),
        }],
    };

    let reg_req = HermesMessage::new_request(
        "host.register",
        serde_json::to_value(RegisterHostParams {
            host: host_info.clone(),
        })
        .unwrap(),
    );

    write_framed_message(&mut host_stream, &reg_req).await;
    let reg_resp = read_framed_message(&mut host_stream).await;

    match reg_resp {
        HermesMessage::Response { result, error, .. } => {
            assert!(error.is_none());
            assert_eq!(result.unwrap()["status"], "registered");
        }
        _ => panic!("Expected Response for host.register"),
    }

    // 2. Connect a simulated Desktop Client
    let mut client_stream = TcpStream::connect(&addr).await.unwrap();

    let topo_req = HermesMessage::new_request("root.query_topology", serde_json::Value::Null);
    write_framed_message(&mut client_stream, &topo_req).await;
    let topo_resp = read_framed_message(&mut client_stream).await;

    match topo_resp {
        HermesMessage::Response { result, error, .. } => {
            assert!(error.is_none());
            let hosts = result.unwrap()["hosts"].clone();
            assert_eq!(hosts.as_array().unwrap().len(), 1);
            assert_eq!(hosts[0]["id"], "host-test-1");
            assert_eq!(hosts[0]["services"][0]["name"], "Telemetry Service");
            assert_eq!(hosts[0]["services"][0]["state"], "running");
        }
        _ => panic!("Expected Response for root.query_topology"),
    }
}

#[tokio::test]
async fn test_root_command_routing_to_host() {
    let (addr, _server_handle) = start_test_server().await;

    // 1. Host connects and registers
    let mut host_stream = TcpStream::connect(&addr).await.unwrap();
    let host_info = HostInfo {
        id: "host-routed".to_string(),
        name: "Routed Host".to_string(),
        hostname: "routed.local".to_string(),
        pid: 100,
        os: "linux".to_string(),
        uptime_secs: 10,
        last_heartbeat: Utc::now(),
        services: vec![ServiceInfo {
            id: "order-proc".to_string(),
            name: "Order Processor".to_string(),
            host_id: "host-routed".to_string(),
            plugin_path: "/lib/order.so".to_string(),
            state: ServiceState::Running,
            uptime_secs: 10,
            thread_count: 2,
            parent_service: None,
            child_services: vec![],
            health_summary: "Nominal".to_string(),
            last_fault: None,
            last_state_change: Utc::now(),
        }],
    };

    let reg_req = HermesMessage::new_request(
        "host.register",
        serde_json::to_value(RegisterHostParams { host: host_info }).unwrap(),
    );
    write_framed_message(&mut host_stream, &reg_req).await;
    let _ = read_framed_message(&mut host_stream).await; // Consume registration response

    // 2. Desktop connects and issues a lifecycle command targeted at host-routed
    let mut client_stream = TcpStream::connect(&addr).await.unwrap();
    let cmd_req = HermesMessage::new_request(
        "service.command",
        serde_json::json!({
            "host_id": "host-routed",
            "service_id": "order-proc",
            "command": ServiceLifecycleCommand::Stop
        }),
    );
    let cmd_id = match cmd_req {
        HermesMessage::Request { ref id, .. } => id.clone(),
        _ => unreachable!(),
    };

    write_framed_message(&mut client_stream, &cmd_req).await;

    // 3. Host receives the forwarded command from Root
    let forwarded_to_host = read_framed_message(&mut host_stream).await;
    match forwarded_to_host {
        HermesMessage::Request { id, method, params } => {
            assert_eq!(id, cmd_id);
            assert_eq!(method, "service.command");
            assert_eq!(params["service_id"], "order-proc");

            // Host executes and replies with success
            let host_reply = HermesMessage::new_response(
                id,
                serde_json::json!({ "acknowledged": true, "new_state": "stopping" }),
            );
            write_framed_message(&mut host_stream, &host_reply).await;
        }
        _ => panic!("Host expected forwarded Request message"),
    }

    // 4. Desktop Client receives the host's reply routed back through Root
    let client_received = read_framed_message(&mut client_stream).await;
    match client_received {
        HermesMessage::Response { id, result, error } => {
            assert_eq!(id, cmd_id);
            assert!(error.is_none());
            assert_eq!(result.unwrap()["new_state"], "stopping");
        }
        _ => panic!("Client expected Response message"),
    }
}

#[tokio::test]
async fn test_command_to_unknown_host_returns_error() {
    let (addr, _server_handle) = start_test_server().await;

    let mut client_stream = TcpStream::connect(&addr).await.unwrap();
    let cmd_req = HermesMessage::new_request(
        "service.command",
        serde_json::json!({
            "host_id": "host-nonexistent",
            "service_id": "any-svc",
            "command": "stop"
        }),
    );

    write_framed_message(&mut client_stream, &cmd_req).await;
    let resp = read_framed_message(&mut client_stream).await;

    match resp {
        HermesMessage::Response { error, .. } => {
            assert!(error.is_some());
            assert_eq!(error.unwrap().code, ErrorCode::HostNotFound);
        }
        _ => panic!("Expected error response for unknown host"),
    }
}

#[tokio::test]
async fn test_broadcast_events_and_logs_to_client() {
    let (addr, _server_handle) = start_test_server().await;

    // 1. Desktop Client connects and subscribes
    let mut client_stream = TcpStream::connect(&addr).await.unwrap();
    let sub_req = HermesMessage::new_request("client.subscribe", serde_json::Value::Null);
    write_framed_message(&mut client_stream, &sub_req).await;
    let sub_resp = read_framed_message(&mut client_stream).await;
    match sub_resp {
        HermesMessage::Response { result, .. } => {
            assert_eq!(result.unwrap()["status"], "subscribed");
        }
        _ => panic!("Expected subscribe response"),
    }

    // 2. Host connects and streams a log entry
    let mut host_stream = TcpStream::connect(&addr).await.unwrap();
    let log_msg = HermesMessage::Log(LogEntry {
        timestamp: Utc::now(),
        level: LogLevel::Warn,
        host_id: "host-logger".to_string(),
        service_id: Some("data-sync".to_string()),
        target: "sync_engine".to_string(),
        message: "High latency detected on upstream connection".to_string(),
    });

    write_framed_message(&mut host_stream, &log_msg).await;

    // 3. Client receives the broadcast log entry in real time
    let received_broadcast = read_framed_message(&mut client_stream).await;
    match received_broadcast {
        HermesMessage::Log(entry) => {
            assert_eq!(entry.host_id, "host-logger");
            assert_eq!(entry.level, LogLevel::Warn);
            assert_eq!(entry.message, "High latency detected on upstream connection");
        }
        _ => panic!("Client expected broadcast Log message"),
    }
}
