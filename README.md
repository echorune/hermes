# HERMES — Rust Service Management Platform

Hermes is an enterprise-grade service management and diagnostic platform consisting of a Python desktop management application built with Dear PyGui and a Rust-based service hosting architecture.

Hermes treats faults as operational investigation and diagnostic events rather than immediate process termination triggers. When a service experiences an unhandled panic, thread failure, or operational degradation, Hermes quarantines the service in a diagnostic state (`FaultDetected`), captures thread call stacks and telemetry, preserves logs, and presents an interactive recovery workflow to the operator.

---

## Architecture Overview

Hermes is composed of four principal layers:

1. **`hermes-core`**: Shared Rust crate containing the C-compatible dynamic plugin ABI (`hermes_abi_v1`), domain models, service lifecycle state machine, and framed JSON wire protocol.
2. **`hermes-root`**: Central management daemon that maintains the cluster registry of hosts and services, tracks connectivity heartbeats, dispatches operator commands, and records operational event history.
3. **`hermes-host`**: Unified service container executable capable of hosting multiple dynamic service libraries (`.so`), managing thread lifecycles, intercepting panics, capturing call stacks, and enforcing dependency-aware startup/shutdown.
4. **`hermes-desktop`**: Native operator control center built in Python 3 with Dear PyGui. Provides real-time service topology trees, thread stack inspector, fault forensics studio, and diagnostic log backup tooling.

```
[ Operator Desktop (Dear PyGui) ]
              │ Framed TCP
              ▼
    [ Root Daemon (hermes-root) ]
         │               │
  Framed │ TCP    Framed │ TCP
         ▼               ▼
[ Host 1 (hermes-host) ] [ Host 2 (hermes-host) ]
   ├── Plugin A (.so)        ├── Parent Plugin (.so)
   └── Plugin B (.so)        └── Child Plugin (.so)
```

---

## Prerequisites

- **Linux** (x86_64 or aarch64)
- **Rust Toolchain:** `rustc` and `cargo` (1.80+)
- **Python:** Python 3.10+ (tested with Python 3.14)
- **C Compiler & Linker:** `gcc` or `clang` (for native plugin cdylibs)

---

## Build Instructions

### 1. Build Rust Binaries and Core Libraries
```bash
cargo build --release
```
This builds:
- `target/release/hermes-root` (Root Management Daemon)
- `target/release/hermes-host` (Service Container Executable)
- `target/release/libhermes_core.rlib` (Core Models & ABI)

### 2. Build Service Plugins
```bash
cargo build --release -p telemetry-service -p order-processor -p inventory-worker -p fault-simulator
```
Dynamic libraries are compiled to `target/release/lib<name>.so`.

### 3. Setup Python Desktop Application
```bash
python3 -m venv venv
./venv/bin/pip install -r hermes-desktop/requirements.txt
```

---

## Running Hermes

### Step 1: Start the Root Management Service
```bash
./target/release/hermes-root --config configs/root.toml
```

### Step 2: Start One or More Service Hosts
Each host runs independently from its own configuration file:

**Host 1 (Single Telemetry Service):**
```bash
./target/release/hermes-host --config configs/host-single-telemetry.toml
```

**Host 2 (Parent-Child Composite Services):**
```bash
./target/release/hermes-host --config configs/host-parent-child.toml
```

**Host 3 (Fault Simulator Service):**
```bash
./target/release/hermes-host --config configs/host-fault-simulator.toml
```

### Step 3: Launch the Operator Desktop Control Center
```bash
./venv/bin/python hermes-desktop/run.py
```

---

## Configuration Examples

### Root Management Configuration (`configs/root.toml`)
```toml
[server]
bind_address = "127.0.0.1:9090"
heartbeat_timeout_secs = 6
event_log_capacity = 1000
```

### Host Instance Configuration (`configs/host-parent-child.toml`)
```toml
[host]
id = "host-production-01"
name = "Production Processing Host"
root_address = "127.0.0.1:9090"
heartbeat_interval_secs = 2

[[plugins]]
id = "order-processor"
name = "Order Processing Pipeline"
path = "target/release/liborder_processor.so"
enabled = true
children = ["inventory-worker"]

[plugins.config]
batch_size = 100
workers = 4

[[plugins]]
id = "inventory-worker"
name = "Inventory Management Worker"
path = "target/release/libinventory_worker.so"
enabled = true
parent = "order-processor"
```

---

## Service Lifecycle & Fault Quarantine

Hermes enforces an explicit state machine:
- `Created` ➔ `Loading` ➔ `Starting` ➔ `Running`
- `Running` ➔ `FaultDetected` ➔ `Diagnosing` ➔ `Stopping` ➔ `Stopped`
- `Stopped` ➔ `StartingAgain` ➔ `Starting`

When a service faults, it remains in `FaultDetected` until the operator inspects the captured call stacks, reviews logs, downloads diagnostic bundles, and explicitly triggers recovery.

---

## Documentation

For comprehensive architectural design, ABI specifications, and implementation phase tracking, see [`PLAN.md`](PLAN.md).
