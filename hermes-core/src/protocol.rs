//! Framed Communication Protocol Specification
//!
//! Provides the length-prefixed streaming protocol for Root <-> Host and Root <-> Desktop UI.
//! Includes JSON-RPC style envelopes, typed commands, notifications, framing codecs, and error types.

use crate::models::{HostInfo, LogEntry, SystemEvent};
use crate::state::ServiceState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Protocol version identifier.
pub const PROTOCOL_VERSION: &str = "hermes.v1";

/// Maximum permitted payload frame size (16 MB) to prevent out-of-memory on malformed input.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Standard protocol error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    UnknownMethod = 1001,
    InvalidParams = 1002,
    HostNotFound = 1003,
    ServiceNotFound = 1004,
    InvalidStateTransition = 1005,
    CommandTimeout = 1006,
    ExecutionFailed = 1007,
    InternalError = 1008,
}

/// Structured protocol error representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("Protocol Error [{code:?}]: {message}")]
pub struct ProtocolErrorPayload {
    pub code: ErrorCode,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// Errors occurring during framing, serialization, or network transport.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Frame size {0} exceeds maximum limit of {MAX_FRAME_SIZE} bytes")]
    FrameTooLarge(usize),

    #[error("JSON serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("UTF-8 decoding error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Remote error [{code:?}]: {message}")]
    Remote {
        code: ErrorCode,
        message: String,
    },
}

/// Commands that can be issued to a hosted service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceLifecycleCommand {
    Start,
    Stop,
    Restart,
    Quiesce,
    ClearFault,
}

/// Top-level message envelope exchanged over framed TCP connections.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HermesMessage {
    /// RPC Request
    Request {
        id: String,
        method: String,
        params: serde_json::Value,
    },
    /// RPC Response
    Response {
        id: String,
        result: Option<serde_json::Value>,
        error: Option<ProtocolErrorPayload>,
    },
    /// Heartbeat signal from a host
    Heartbeat {
        host_id: String,
        timestamp: DateTime<Utc>,
    },
    /// Asynchronous system or service event
    Event(SystemEvent),
    /// Asynchronous streaming log entry
    Log(LogEntry),
}

impl HermesMessage {
    /// Creates a new RPC request message.
    pub fn new_request(method: impl Into<String>, params: serde_json::Value) -> Self {
        HermesMessage::Request {
            id: uuid::Uuid::new_v4().to_string(),
            method: method.into(),
            params,
        }
    }

    /// Creates a success response for a given request ID.
    pub fn new_response(id: impl Into<String>, result: serde_json::Value) -> Self {
        HermesMessage::Response {
            id: id.into(),
            result: Some(result),
            error: None,
        }
    }

    /// Creates an error response for a given request ID.
    pub fn new_error_response(
        id: impl Into<String>,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> Self {
        HermesMessage::Response {
            id: id.into(),
            result: None,
            error: Some(ProtocolErrorPayload {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Typed RPC Request / Response Parameter Helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterHostParams {
    pub host: HostInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateServiceStateParams {
    pub host_id: String,
    pub service_id: String,
    pub state: ServiceState,
    pub health_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCommandParams {
    pub host_id: String,
    pub service_id: String,
    pub command: ServiceLifecycleCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetThreadStacksParams {
    pub host_id: String,
    pub service_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupLogsParams {
    pub host_id: String,
    pub service_id: Option<String>,
    pub destination_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupLogsResult {
    pub archive_path: String,
    pub file_count: usize,
    pub byte_size: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyQueryResult {
    pub hosts: Vec<HostInfo>,
}

// ---------------------------------------------------------------------------
// Framing Codec (4-byte Big-Endian Length Prefix + UTF-8 JSON)
// ---------------------------------------------------------------------------

/// Encodes a `HermesMessage` into a length-prefixed binary buffer.
pub fn encode_framed_message(msg: &HermesMessage) -> Result<Vec<u8>, ProtocolError> {
    let payload = serde_json::to_vec(msg)?;
    let length = payload.len();

    if length > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge(length));
    }

    let mut buffer = Vec::with_capacity(4 + length);
    buffer.extend_from_slice(&(length as u32).to_be_bytes());
    buffer.extend_from_slice(&payload);
    Ok(buffer)
}

/// Attempts to decode a `HermesMessage` from a byte slice.
///
/// Returns:
/// - `Ok(Some((message, consumed_bytes)))` if a full frame was parsed.
/// - `Ok(None)` if more data is needed to complete the frame.
/// - `Err(ProtocolError)` if the frame length exceeds limits or JSON is malformed.
pub fn decode_framed_message(buffer: &[u8]) -> Result<Option<(HermesMessage, usize)>, ProtocolError> {
    if buffer.len() < 4 {
        return Ok(None);
    }

    let length = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

    if length > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge(length));
    }

    let total_required = 4 + length;
    if buffer.len() < total_required {
        return Ok(None);
    }

    let json_bytes = &buffer[4..total_required];
    let message: HermesMessage = serde_json::from_slice(json_bytes)?;
    Ok(Some((message, total_required)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_framed_message_roundtrip() {
        let req = HermesMessage::new_request(
            "service.command",
            serde_json::json!({
                "host_id": "host-1",
                "service_id": "order-proc",
                "command": "start"
            }),
        );

        let encoded = encode_framed_message(&req).expect("Failed to encode");
        assert!(encoded.len() > 4);

        let (decoded, consumed) = decode_framed_message(&encoded)
            .expect("Failed to decode")
            .expect("Incomplete frame");

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_partial_frame_decoding() {
        let msg = HermesMessage::Heartbeat {
            host_id: "host-99".to_string(),
            timestamp: Utc::now(),
        };

        let encoded = encode_framed_message(&msg).unwrap();

        // Pass only first 3 bytes (incomplete length header)
        assert!(decode_framed_message(&encoded[0..3]).unwrap().is_none());

        // Pass only header + half of payload
        let mid = 4 + (encoded.len() - 4) / 2;
        assert!(decode_framed_message(&encoded[0..mid]).unwrap().is_none());

        // Pass full buffer
        let (decoded, consumed) = decode_framed_message(&encoded).unwrap().unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_frame_size_overflow_guard() {
        let huge_len = (MAX_FRAME_SIZE + 100) as u32;
        let mut malformed_header = huge_len.to_be_bytes().to_vec();
        malformed_header.extend_from_slice(b"extra bytes");

        let err = decode_framed_message(&malformed_header).unwrap_err();
        match err {
            ProtocolError::FrameTooLarge(sz) => assert_eq!(sz, MAX_FRAME_SIZE + 100),
            _ => panic!("Expected FrameTooLarge error"),
        }
    }
}
