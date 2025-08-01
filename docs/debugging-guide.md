# Lightning DB Debugging Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Debug Build Configuration](#debug-build-configuration)
3. [Logging and Tracing](#logging-and-tracing)
4. [Common Issues and Solutions](#common-issues-and-solutions)
5. [Debugging Tools](#debugging-tools)
6. [Core Component Debugging](#core-component-debugging)
7. [Transaction Debugging](#transaction-debugging)
8. [Performance Debugging](#performance-debugging)
9. [Data Corruption Debugging](#data-corruption-debugging)
10. [Production Debugging](#production-debugging)

---

## Introduction

This guide provides comprehensive debugging techniques for Lightning DB issues in development and production environments.

### Debugging Principles

1. **Reproduce reliably** - Create minimal test case
2. **Gather evidence** - Logs, traces, core dumps
3. **Form hypothesis** - Based on evidence
4. **Test systematically** - Isolate variables
5. **Document findings** - For future reference

---

## Debug Build Configuration

### Enabling Debug Features

```toml
# Cargo.toml
[features]
debug_assertions = ["lightning_db/debug_assertions"]
trace_logging = ["lightning_db/trace_logging"]
debug_tools = ["lightning_db/debug_tools"]
sanitizers = ["lightning_db/sanitizers"]

[profile.debug-opt]
inherits = "release"
debug = true
debug-assertions = true
overflow-checks = true
```

### Debug Build Commands

```bash
# Build with debug symbols
cargo build --features "debug_assertions,trace_logging"

# Build with sanitizers
RUSTFLAGS="-Z sanitizer=address" cargo build --target x86_64-unknown-linux-gnu

# Build with full debug info
cargo build --profile debug-opt
```

### Runtime Debug Options

```rust
use lightning_db::{Database, DebugConfig};

let debug_config = DebugConfig {
    enable_assertions: true,
    trace_operations: true,
    validate_checksums: true,
    track_allocations: true,
    slow_operation_threshold: Duration::from_millis(100),
};

let db = Database::open_with_debug("./mydb", config, debug_config)?;
```

---

## Logging and Tracing

### Structured Logging

```rust
use tracing::{info, warn, error, debug, trace, span, Level};

// Initialize tracing
tracing_subscriber::fmt()
    .with_max_level(Level::TRACE)
    .with_thread_ids(true)
    .with_line_number(true)
    .with_file(true)
    .json()
    .init();

// Use structured logging
let span = span!(Level::INFO, "database_operation", 
    operation = "put",
    key_size = key.len(),
    value_size = value.len()
);

let _enter = span.enter();
```

### Log Levels and Filtering

```bash
# Set log level via environment
RUST_LOG=lightning_db=debug,lightning_db::btree=trace cargo run

# Filter specific modules
RUST_LOG=lightning_db::transaction=debug,lightning_db::storage=trace

# Enable all logs
RUST_LOG=trace

# Production-safe logging
RUST_LOG=lightning_db=info,warn
```

### Operation Tracing

```rust
// Enable operation tracing
db.enable_operation_tracing()?;

// Trace specific operation
let trace_id = db.start_trace("complex_query")?;
// ... perform operations ...
let trace = db.end_trace(trace_id)?;

// Analyze trace
println!("Operation trace:");
for event in trace.events {
    println!("  {} +{}ms: {}", 
        event.timestamp, 
        event.elapsed_ms, 
        event.description
    );
}
```

---

## Common Issues and Solutions

### 1. Database Won't Open

**Symptoms**: Error opening database, corruption messages

**Debug Steps**:
```rust
// Check database state
use lightning_db::debug::DatabaseInspector;

let inspector = DatabaseInspector::new("./mydb")?;
let report = inspector.inspect()?;

println!("Database state: {:?}", report.state);
println!("Last clean shutdown: {}", report.clean_shutdown);
println!("WAL entries: {}", report.wal_entries);
println!("Data pages: {}", report.data_pages);

// Attempt repair
if !report.clean_shutdown {
    println!("Attempting recovery...");
    let recovery_result = Database::recover("./mydb")?;
    println!("Recovered {} transactions", recovery_result.recovered_count);
}
```

### 2. Transaction Failures

**Symptoms**: Transactions abort unexpectedly

**Debug Steps**:
```rust
// Enable transaction debugging
db.set_transaction_debug(true)?;

// Trace transaction lifecycle
let mut tx = db.begin_transaction_traced()?;
tx.put(b"key", b"value")?;

// Check conflicts
if let Err(e) = tx.commit() {
    let debug_info = tx.get_debug_info()?;
    println!("Transaction failed: {:?}", e);
    println!("Conflicts: {:?}", debug_info.conflicts);
    println!("Read set: {:?}", debug_info.read_set);
    println!("Write set: {:?}", debug_info.write_set);
}
```

### 3. Memory Leaks

**Symptoms**: Growing memory usage, OOM errors

**Debug Steps**:
```rust
// Enable memory tracking
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Track allocations
use lightning_db::debug::AllocationTracker;

let tracker = AllocationTracker::new();
tracker.start()?;

// ... run operations ...

let report = tracker.stop()?;
println!("Allocations: {}", report.total_allocations);
println!("Deallocations: {}", report.total_deallocations);
println!("Leaked bytes: {}", report.leaked_bytes);

// Find leak sources
for leak in report.leaks {
    println!("Leak: {} bytes at {}", leak.size, leak.backtrace);
}
```

### 4. Slow Operations

**Symptoms**: High latency, timeouts

**Debug Steps**:
```rust
// Profile slow operations
db.set_slow_operation_callback(|op| {
    if op.duration > Duration::from_millis(100) {
        eprintln!("Slow operation: {} took {:?}", op.name, op.duration);
        eprintln!("Stack trace: {}", op.backtrace);
    }
})?;

// Trace operation breakdown
let breakdown = db.trace_operation_breakdown(|| {
    db.get(b"key")
})?;

println!("Operation breakdown:");
println!("  Parse: {:?}", breakdown.parse_time);
println!("  Lock acquisition: {:?}", breakdown.lock_time);
println!("  I/O: {:?}", breakdown.io_time);
println!("  Deserialization: {:?}", breakdown.deserialize_time);
```

---

## Debugging Tools

### Built-in Debug Commands

```rust
use lightning_db::debug::DebugShell;

// Start interactive debug shell
let shell = DebugShell::new(&db);
shell.run()?;

// Available commands:
// > info              - Show database info
// > stats             - Show statistics
// > get <key>         - Get value and metadata
// > scan <prefix>     - Scan keys with prefix
// > page <id>         - Inspect page
// > btree             - Show B+tree structure
// > transactions      - List active transactions
// > locks             - Show lock information
// > cache             - Cache statistics
// > compact           - Trigger compaction
```

### External Tools

#### GDB Integration
```bash
# Run with GDB
gdb ./target/debug/myapp
(gdb) break lightning_db::btree::BTree::insert
(gdb) run
(gdb) bt full  # Full backtrace when breakpoint hit

# Pretty printing
(gdb) source ./scripts/gdb_lightning_db.py
(gdb) p my_database
$1 = Database { 
    path: "./mydb",
    page_count: 1523,
    cache_size: 104857600,
    transaction_count: 3
}
```

#### LLDB Integration
```bash
# Run with LLDB
lldb ./target/debug/myapp
(lldb) b lightning_db::storage::PageManager::allocate_page
(lldb) r
(lldb) bt all  # All threads backtrace

# Custom formatters
(lldb) command script import ./scripts/lldb_formatters.py
```

#### rr (Record & Replay)
```bash
# Record execution
rr record ./target/debug/myapp

# Replay and debug
rr replay
(gdb) watch -l some_variable
(gdb) reverse-continue  # Go back in time!
```

---

## Core Component Debugging

### B+Tree Debugging

```rust
// Enable B+tree validation
db.set_btree_validation(true)?;

// Dump tree structure
let tree_dump = db.dump_btree_structure()?;
println!("{}", tree_dump.to_graphviz());

// Validate tree invariants
let validation_result = db.validate_btree()?;
if !validation_result.is_valid() {
    for error in validation_result.errors {
        eprintln!("B+tree error: {}", error);
    }
}

// Trace tree operations
db.trace_btree_operations(|op| {
    println!("B+tree op: {:?}", op);
});
```

### Page Manager Debugging

```rust
// Inspect page allocation
let page_stats = db.get_page_statistics()?;
println!("Total pages: {}", page_stats.total_pages);
println!("Free pages: {}", page_stats.free_pages);
println!("Fragmentation: {:.2}%", page_stats.fragmentation * 100.0);

// Dump specific page
let page_dump = db.dump_page(page_id)?;
println!("Page {} dump:", page_id);
println!("  Type: {:?}", page_dump.page_type);
println!("  Checksum: {:x}", page_dump.checksum);
println!("  Data: {:?}", page_dump.data);

// Verify page integrity
let integrity_check = db.verify_all_pages()?;
for error in integrity_check.errors {
    eprintln!("Page error: {}", error);
}
```

### Cache Debugging

```rust
// Cache access trace
db.enable_cache_tracing()?;

// Get cache debug info
let cache_debug = db.get_cache_debug_info()?;
println!("Cache entries: {}", cache_debug.entry_count);
println!("Hot keys: {:?}", cache_debug.hot_keys);
println!("Cold keys: {:?}", cache_debug.cold_keys);
println!("Ghost entries: {}", cache_debug.ghost_count);

// Simulate cache behavior
let simulator = CacheSimulator::new(cache_debug.access_trace);
let optimal_size = simulator.find_optimal_size()?;
println!("Optimal cache size: {} MB", optimal_size / 1024 / 1024);
```

---

## Transaction Debugging

### MVCC Debugging

```rust
// Enable MVCC tracing
db.set_mvcc_trace(true)?;

// Inspect version chain
let version_chain = db.get_version_chain(key)?;
println!("Version chain for key {:?}:", key);
for version in version_chain {
    println!("  Transaction {}: {:?}", version.txn_id, version.value);
}

// Check for conflicts
let conflict_graph = db.build_conflict_graph()?;
if conflict_graph.has_cycles() {
    println!("Deadlock detected!");
    for cycle in conflict_graph.find_cycles() {
        println!("  Cycle: {:?}", cycle);
    }
}
```

### Lock Debugging

```rust
// Deadlock detection
db.enable_deadlock_detection()?;

// Monitor lock waits
db.set_lock_wait_callback(|wait_info| {
    if wait_info.duration > Duration::from_secs(1) {
        eprintln!("Long lock wait: {:?} waiting for {:?}", 
            wait_info.waiter, wait_info.holder);
    }
})?;

// Dump lock table
let lock_table = db.dump_lock_table()?;
for (resource, lock_info) in lock_table {
    println!("Resource {}: {} readers, {} writers waiting", 
        resource, lock_info.readers, lock_info.writers_waiting);
}
```

---

## Performance Debugging

### Latency Analysis

```rust
// Enable latency breakdown
db.enable_latency_breakdown()?;

// Analyze operation latency
let latency = db.analyze_operation_latency(|| {
    db.put(key, value)
})?;

println!("Latency breakdown:");
println!("  Validation: {:?}", latency.validation);
println!("  Serialization: {:?}", latency.serialization);
println!("  Lock acquisition: {:?}", latency.locking);
println!("  Page allocation: {:?}", latency.allocation);
println!("  I/O write: {:?}", latency.io_write);
println!("  WAL sync: {:?}", latency.wal_sync);
println!("  Index update: {:?}", latency.index_update);
```

### Bottleneck Identification

```rust
// Profile hot paths
let profile = db.profile_hot_paths(Duration::from_secs(60))?;

println!("Top CPU consumers:");
for (function, percentage) in profile.top_functions(10) {
    println!("  {}: {:.1}%", function, percentage);
}

println!("\nLock contention:");
for (lock, stats) in profile.lock_contention() {
    println!("  {}: {:.1}% contended", lock, stats.contention_rate * 100.0);
}
```

---

## Data Corruption Debugging

### Corruption Detection

```rust
// Full database verification
let verification = db.verify_full()?;

if !verification.is_clean() {
    println!("Corruption detected:");
    for issue in verification.issues {
        println!("  {}: {}", issue.severity, issue.description);
        println!("    Location: {}", issue.location);
        println!("    Impact: {} keys affected", issue.affected_keys);
    }
}

// Check specific corruption types
db.check_page_checksums()?;
db.verify_btree_structure()?;
db.validate_transaction_log()?;
db.check_reference_counts()?;
```

### Recovery Procedures

```rust
// Attempt automatic recovery
let recovery_options = RecoveryOptions {
    aggressive: true,
    preserve_data: true,
    rebuild_indexes: true,
};

let recovery_result = db.recover_corrupted(recovery_options)?;
println!("Recovery complete:");
println!("  Recovered: {} keys", recovery_result.recovered_keys);
println!("  Lost: {} keys", recovery_result.lost_keys);
println!("  Repaired: {} pages", recovery_result.repaired_pages);

// Export salvageable data
if recovery_result.lost_keys > 0 {
    db.export_salvageable("salvaged_data.json")?;
}
```

---

## Production Debugging

### Non-Intrusive Debugging

```rust
// Attach to running database
let debugger = DatabaseDebugger::attach("./prod_db", ReadOnly)?;

// Take snapshot for offline analysis
let snapshot = debugger.create_snapshot("debug_snapshot")?;

// Analyze without impacting production
let analysis = snapshot.analyze()?;
println!("Issues found: {}", analysis.issue_count);
```

### Remote Debugging

```rust
// Enable debug server
db.start_debug_server("127.0.0.1:9999")?;

// Connect from remote machine
let client = DebugClient::connect("prod-server:9999")?;

// Query database state
let stats = client.get_statistics()?;
let slow_ops = client.get_slow_operations()?;
let lock_info = client.get_lock_info()?;

// Trigger diagnostics
client.run_diagnostic("memory_check")?;
client.collect_profile(Duration::from_secs(30))?;
```

### Core Dump Analysis

```bash
# Enable core dumps
ulimit -c unlimited

# Configure core pattern
echo "/tmp/cores/core.%e.%p.%t" > /proc/sys/kernel/core_pattern

# Analyze core dump
gdb ./myapp /tmp/cores/core.myapp.12345.1234567890
(gdb) bt
(gdb) thread apply all bt
(gdb) info locals
(gdb) p *database
```

---

## Debug Checklists

### Pre-Debug Checklist
- [ ] Reproduce issue consistently
- [ ] Create minimal test case
- [ ] Enable debug logging
- [ ] Set up debug environment
- [ ] Backup production data

### During Debug
- [ ] Document steps taken
- [ ] Save all logs and traces
- [ ] Test hypotheses systematically
- [ ] Check for similar issues
- [ ] Consider edge cases

### Post-Debug
- [ ] Write regression test
- [ ] Update documentation
- [ ] Share findings with team
- [ ] Add monitoring for issue
- [ ] Plan preventive measures

---

## Debug Output Examples

### Transaction Debug Output
```
[2025-01-15 10:23:45.123] TRACE lightning_db::transaction: Begin transaction 42
[2025-01-15 10:23:45.124] DEBUG lightning_db::transaction: Read key="user:123" version=15
[2025-01-15 10:23:45.125] DEBUG lightning_db::transaction: Write key="user:123" value_size=256
[2025-01-15 10:23:45.126] WARN  lightning_db::transaction: Conflict detected: key="user:123" 
  current_version=16 read_version=15
[2025-01-15 10:23:45.127] DEBUG lightning_db::transaction: Abort transaction 42: WriteConflict
```

### Page Allocation Debug Output
```
[2025-01-15 10:23:45.200] TRACE lightning_db::storage: Allocate page request
[2025-01-15 10:23:45.201] DEBUG lightning_db::storage: Free list empty, extending file
[2025-01-15 10:23:45.210] DEBUG lightning_db::storage: Allocated page_id=1523 offset=6234112
[2025-01-15 10:23:45.211] TRACE lightning_db::storage: Page header: type=Leaf, checksum=0xDEADBEEF
```

---

For more debugging scenarios, see the [Troubleshooting Guide](./PRODUCTION_TROUBLESHOOTING_GUIDE.md).