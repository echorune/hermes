//! Service Worker Thread Manager & Multi-Tiered Stack Capture Engine
//!
//! Tracks registered worker threads per service, catches unhandled thread panics,
//! captures backtraces, and inspects Linux kernel thread stacks via `/proc/self/task/[tid]/stack`.

use chrono::Utc;
use hermes_core::models::{CallStackFrame, CallStackSnapshot, ThreadInfo, ThreadState};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, RwLock};
use std::thread;
use tracing::{debug, warn};

/// Obtains the Linux OS Thread ID (TID) for the calling thread.
pub fn current_thread_id() -> u64 {
    unsafe { libc::syscall(libc::SYS_gettid) as u64 }
}

/// Managed entry representing an individual active or faulted thread.
#[derive(Debug, Clone)]
pub struct MonitoredThread {
    pub thread_id: u64,
    pub name: String,
    pub service_id: String,
    pub state: ThreadState,
    pub cpu_usage_pct: f32,
    pub is_worker: bool,
    pub last_seen: chrono::DateTime<Utc>,
    pub last_stack: Option<CallStackSnapshot>,
}

impl MonitoredThread {
    pub fn to_thread_info(&self) -> ThreadInfo {
        ThreadInfo {
            id: self.thread_id,
            name: self.name.clone(),
            state: self.state,
            cpu_usage_pct: self.cpu_usage_pct,
            is_worker: self.is_worker,
            last_seen: self.last_seen,
        }
    }
}

pub type FaultCallback = Arc<dyn Fn(String, u64, String, String, CallStackSnapshot) + Send + Sync>;

#[derive(Clone)]
pub struct ThreadManager {
    threads: Arc<RwLock<HashMap<u64, MonitoredThread>>>,
    fault_callback: Arc<RwLock<Option<FaultCallback>>>,
}

impl ThreadManager {
    pub fn new() -> Self {
        Self {
            threads: Arc::new(RwLock::new(HashMap::new())),
            fault_callback: Arc::new(RwLock::new(None)),
        }
    }

    /// Sets a callback invoked when a managed thread panics.
    pub fn set_fault_callback<F>(&self, callback: F)
    where
        F: Fn(String, u64, String, String, CallStackSnapshot) + Send + Sync + 'static,
    {
        let mut cb = self.fault_callback.write().unwrap();
        *cb = Some(Arc::new(callback));
    }

    /// Registers a thread into the registry.
    pub fn register_thread(&self, service_id: &str, thread_id: u64, name: &str, is_worker: bool) {
        let mut threads = self.threads.write().unwrap();
        threads.insert(
            thread_id,
            MonitoredThread {
                thread_id,
                name: name.to_string(),
                service_id: service_id.to_string(),
                state: ThreadState::Running,
                cpu_usage_pct: 0.0,
                is_worker,
                last_seen: Utc::now(),
                last_stack: None,
            },
        );
        debug!(
            service_id = %service_id,
            tid = thread_id,
            name = %name,
            "Registered monitored thread"
        );
    }

    /// Updates thread state.
    pub fn update_thread_state(&self, thread_id: u64, state: ThreadState) {
        let mut threads = self.threads.write().unwrap();
        if let Some(t) = threads.get_mut(&thread_id) {
            t.state = state;
            t.last_seen = Utc::now();
        }
    }

    /// Returns thread info list for a specific service.
    pub fn get_service_threads(&self, service_id: &str) -> Vec<ThreadInfo> {
        let threads = self.threads.read().unwrap();
        threads
            .values()
            .filter(|t| t.service_id == service_id)
            .map(|t| t.to_thread_info())
            .collect()
    }

    /// Returns all registered threads across all services.
    pub fn get_all_threads(&self) -> Vec<ThreadInfo> {
        let threads = self.threads.read().unwrap();
        threads.values().map(|t| t.to_thread_info()).collect()
    }

    /// Spawns a supervised worker thread that intercepts panics, preserves backtraces,
    /// and marks the thread as Faulted rather than aborting the process.
    pub fn spawn_worker<F>(
        &self,
        service_id: impl Into<String>,
        thread_name: impl Into<String>,
        worker_fn: F,
    ) -> Result<thread::JoinHandle<()>, std::io::Error>
    where
        F: FnOnce() + Send + 'static,
    {
        let svc_id = service_id.into();
        let t_name = thread_name.into();
        let this = self.clone();

        let svc_id_clone = svc_id.clone();
        let t_name_clone = t_name.clone();

        thread::Builder::new()
            .name(t_name.clone())
            .spawn(move || {
                let tid = current_thread_id();
                this.register_thread(&svc_id_clone, tid, &t_name_clone, true);

                // Run worker inside catch_unwind
                let result = catch_unwind(AssertUnwindSafe(worker_fn));

                match result {
                    Ok(()) => {
                        this.update_thread_state(tid, ThreadState::Terminated);
                        debug!(
                            service_id = %svc_id_clone,
                            tid = tid,
                            name = %t_name_clone,
                            "Worker thread terminated normally"
                        );
                    }
                    Err(panic_payload) => {
                        let backtrace = std::backtrace::Backtrace::capture();
                        let error_msg = extract_panic_message(&panic_payload);

                        warn!(
                            service_id = %svc_id_clone,
                            tid = tid,
                            thread = %t_name_clone,
                            panic = %error_msg,
                            "Caught unhandled panic in worker thread! Preserving diagnostic backtrace."
                        );

                        // Build structured call stack snapshot
                        let snapshot = parse_backtrace_snapshot(
                            tid,
                            &t_name_clone,
                            &backtrace,
                        );

                        // Update thread state to Faulted and cache stack
                        {
                            let mut threads = this.threads.write().unwrap();
                            if let Some(t) = threads.get_mut(&tid) {
                                t.state = ThreadState::Faulted;
                                t.last_stack = Some(snapshot.clone());
                                t.last_seen = Utc::now();
                            }
                        }

                        // Notify fault callback if installed
                        let cb_opt = {
                            let cb_lock = this.fault_callback.read().unwrap();
                            cb_lock.clone()
                        };

                        if let Some(cb) = cb_opt {
                            cb(
                                svc_id_clone.clone(),
                                tid,
                                t_name_clone.clone(),
                                error_msg,
                                snapshot,
                            );
                        }
                    }
                }
            })
    }

    /// Captures the call stack for a specific thread.
    ///
    /// Multi-tiered strategy:
    /// 1. If thread previously panicked, returns cached panic backtrace snapshot.
    /// 2. Attempts to read Linux kernel call stack from `/proc/self/task/[tid]/stack`.
    /// 3. Returns fallback active snapshot.
    pub fn capture_thread_stack(&self, tid: u64) -> CallStackSnapshot {
        // 1. Check if cached stack exists (e.g. from panic)
        {
            let threads = self.threads.read().unwrap();
            if let Some(t) = threads.get(&tid) {
                if let Some(ref stack) = t.last_stack {
                    return stack.clone();
                }
            }
        }

        let thread_name = {
            let threads = self.threads.read().unwrap();
            threads
                .get(&tid)
                .map(|t| t.name.clone())
                .unwrap_or_else(|| format!("thread-{}", tid))
        };

        // 2. Read Linux kernel stack via procfs
        let proc_stack_path = format!("/proc/self/task/{}/stack", tid);
        if let Ok(file) = File::open(&proc_stack_path) {
            let reader = BufReader::new(file);
            let mut frames = Vec::new();

            for (idx, line_res) in reader.lines().enumerate() {
                if let Ok(line) = line_res {
                    let cleaned = line.trim();
                    if !cleaned.is_empty() {
                        let symbol = if let Some(pos) = cleaned.find(' ') {
                            cleaned[pos + 1..].to_string()
                        } else {
                            cleaned.to_string()
                        };

                        frames.push(CallStackFrame {
                            index: idx,
                            symbol,
                            file: Some("[kernel space]".to_string()),
                            line: None,
                            col: None,
                        });
                    }
                }
            }

            if !frames.is_empty() {
                return CallStackSnapshot {
                    captured_at: Utc::now(),
                    thread_id: tid,
                    thread_name,
                    frames,
                    raw_backtrace: None,
                };
            }
        }

        // 3. Fallback: current thread capture if target is self, or running state
        let backtrace = std::backtrace::Backtrace::capture();
        parse_backtrace_snapshot(tid, &thread_name, &backtrace)
    }

    /// Captures call stacks across all threads belonging to a service.
    pub fn capture_all_service_stacks(&self, service_id: &str) -> Vec<CallStackSnapshot> {
        let thread_ids: Vec<u64> = {
            let threads = self.threads.read().unwrap();
            threads
                .values()
                .filter(|t| t.service_id == service_id)
                .map(|t| t.thread_id)
                .collect()
        };

        thread_ids
            .into_iter()
            .map(|tid| self.capture_thread_stack(tid))
            .collect()
    }
}

/// Extracts panic payload string safely.
fn extract_panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic payload".to_string()
    }
}

/// Converts a `std::backtrace::Backtrace` into a structured `CallStackSnapshot`.
fn parse_backtrace_snapshot(
    thread_id: u64,
    thread_name: &str,
    backtrace: &std::backtrace::Backtrace,
) -> CallStackSnapshot {
    let raw = format!("{:#?}", backtrace);
    let mut frames = Vec::new();

    // Parse frames from backtrace string representation
    for (idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("at ") || trimmed.contains("::") {
            frames.push(CallStackFrame {
                index: idx,
                symbol: trimmed.to_string(),
                file: None,
                line: None,
                col: None,
            });
        }
    }

    if frames.is_empty() {
        frames.push(CallStackFrame {
            index: 0,
            symbol: "<captured thread frame>".to_string(),
            file: None,
            line: None,
            col: None,
        });
    }

    CallStackSnapshot {
        captured_at: Utc::now(),
        thread_id,
        thread_name: thread_name.to_string(),
        frames,
        raw_backtrace: Some(raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_current_thread_id() {
        let tid = current_thread_id();
        assert!(tid > 0);
    }

    #[test]
    fn test_thread_registration_and_query() {
        let manager = ThreadManager::new();
        manager.register_thread("svc-1", 1234, "worker-1", true);
        manager.register_thread("svc-1", 1235, "worker-2", true);
        manager.register_thread("svc-2", 1236, "other-worker", true);

        let svc1_threads = manager.get_service_threads("svc-1");
        assert_eq!(svc1_threads.len(), 2);
        assert_eq!(svc1_threads[0].name, "worker-1");
        assert_eq!(svc1_threads[1].name, "worker-2");

        let all_threads = manager.get_all_threads();
        assert_eq!(all_threads.len(), 3);
    }

    #[test]
    fn test_worker_panic_interception_and_backtrace_capture() {
        let manager = ThreadManager::new();
        let fault_received = Arc::new(AtomicBool::new(false));
        let fault_received_clone = fault_received.clone();

        manager.set_fault_callback(move |svc_id, tid, thread_name, error_msg, snapshot| {
            assert_eq!(svc_id, "test-panic-svc");
            assert_eq!(thread_name, "panicking-worker");
            assert!(error_msg.contains("Deliberate test panic"));
            assert!(!snapshot.frames.is_empty());
            assert!(tid > 0);
            fault_received_clone.store(true, Ordering::SeqCst);
        });

        let handle = manager
            .spawn_worker("test-panic-svc", "panicking-worker", || {
                panic!("Deliberate test panic in monitored worker!");
            })
            .expect("Failed to spawn worker");

        let _ = handle.join();

        assert!(fault_received.load(Ordering::SeqCst));

        // Thread must be in Faulted state
        let threads = manager.get_service_threads("test-panic-svc");
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].state, ThreadState::Faulted);
    }
}
