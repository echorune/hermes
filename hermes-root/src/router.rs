//! Command Router
//!
//! Dispatches commands from Desktop UI to target service hosts,
//! tracks in-flight request IDs via oneshot channels, and enforces timeouts.

use hermes_core::protocol::{ErrorCode, HermesMessage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

pub struct CommandRouter {
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<HermesMessage>>>>,
    default_timeout: Duration,
}

impl CommandRouter {
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            default_timeout,
        }
    }

    /// Dispatches a message to a host and waits for the matching response.
    pub async fn dispatch_to_host(
        &self,
        request_id: String,
        host_sender: mpsc::Sender<HermesMessage>,
        message: HermesMessage,
    ) -> HermesMessage {
        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending.lock().await;
            pending.insert(request_id.clone(), tx);
        }

        // Send down the host channel
        if let Err(e) = host_sender.send(message).await {
            warn!(request_id = %request_id, error = %e, "Failed to send command to host channel");
            let mut pending = self.pending.lock().await;
            pending.remove(&request_id);
            return HermesMessage::new_error_response(
                request_id,
                ErrorCode::ExecutionFailed,
                format!("Failed to deliver command to host: {}", e),
            );
        }

        // Await matching response with timeout
        match tokio::time::timeout(self.default_timeout, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&request_id);
                HermesMessage::new_error_response(
                    request_id,
                    ErrorCode::ExecutionFailed,
                    "Host connection dropped while awaiting response",
                )
            }
            Err(_) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&request_id);
                warn!(request_id = %request_id, "Command dispatch timed out");
                HermesMessage::new_error_response(
                    request_id,
                    ErrorCode::CommandTimeout,
                    format!("Command timed out after {} seconds", self.default_timeout.as_secs()),
                )
            }
        }
    }

    /// Delivers an incoming response from a host to the awaiting request listener.
    pub async fn complete_request(&self, request_id: &str, response: HermesMessage) -> bool {
        let mut pending = self.pending.lock().await;
        if let Some(tx) = pending.remove(request_id) {
            let _ = tx.send(response);
            debug!(request_id = %request_id, "Completed routed request");
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_successful_dispatch_and_completion() {
        let router = CommandRouter::new(Duration::from_secs(2));
        let (host_tx, mut host_rx) = mpsc::channel(5);

        let req_id = "test-req-1".to_string();
        let req_id_clone = req_id.clone();

        // Spawn mock host listener
        tokio::spawn(async move {
            if let Some(msg) = host_rx.recv().await {
                if let HermesMessage::Request { id, .. } = msg {
                    assert_eq!(id, req_id_clone);
                }
            }
        });

        let router_clone = CommandRouter {
            pending: router.pending.clone(),
            default_timeout: router.default_timeout,
        };

        // Complete request in background task
        let req_id_complete = req_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            router_clone
                .complete_request(
                    &req_id_complete,
                    HermesMessage::new_response(
                        &req_id_complete,
                        serde_json::json!({ "success": true }),
                    ),
                )
                .await;
        });

        let resp = router
            .dispatch_to_host(
                req_id.clone(),
                host_tx,
                HermesMessage::new_request("test.method", serde_json::Value::Null),
            )
            .await;

        match resp {
            HermesMessage::Response { id, result, error } => {
                assert_eq!(id, req_id);
                assert!(error.is_none());
                assert_eq!(result.unwrap()["success"], true);
            }
            _ => panic!("Expected Response message"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_timeout() {
        let router = CommandRouter::new(Duration::from_millis(50));
        let (host_tx, _host_rx) = mpsc::channel(5);

        let resp = router
            .dispatch_to_host(
                "timeout-req".to_string(),
                host_tx,
                HermesMessage::new_request("test.timeout", serde_json::Value::Null),
            )
            .await;

        match resp {
            HermesMessage::Response { error, .. } => {
                assert!(error.is_some());
                assert_eq!(error.unwrap().code, ErrorCode::CommandTimeout);
            }
            _ => panic!("Expected error response"),
        }
    }
}
