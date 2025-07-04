# Lightning DB Development Guide

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Environment](#development-environment)
3. [Project Structure](#project-structure)
4. [Building & Testing](#building--testing)
5. [Contributing Guidelines](#contributing-guidelines)
6. [Code Standards](#code-standards)
7. [Architecture Guidelines](#architecture-guidelines)
8. [Performance Optimization](#performance-optimization)
9. [Debugging & Profiling](#debugging--profiling)
10. [Release Process](#release-process)

## Getting Started

### Prerequisites

#### System Requirements
- **Rust**: 1.70+ (latest stable recommended)
- **OS**: Linux, macOS, or Windows
- **Memory**: 8+ GB RAM for development
- **Storage**: 10+ GB for dependencies and builds

#### Required Tools
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install additional tools
cargo install cargo-watch
cargo install cargo-flamegraph
cargo install cargo-criterion
cargo install cargo-audit
cargo install cargo-deny
cargo install cargo-udeps

# Platform-specific tools
# Linux:
sudo apt-get install build-essential pkg-config libssl-dev
# macOS:
xcode-select --install
# Windows:
# Install Visual Studio Build Tools
```

### Quick Start

```bash
# Clone repository
git clone https://github.com/org/lightning-db.git
cd lightning-db

# Build
cargo build

# Run tests
cargo test

# Run examples
cargo run --example basic_usage
cargo run --example final_benchmark
```

## Development Environment

### IDE Setup

#### VS Code (Recommended)
```json
// .vscode/settings.json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy",
    "rust-analyzer.cargo.allFeatures": true,
    "editor.formatOnSave": true,
    "files.associations": {
        "*.rs": "rust"
    }
}
```

#### Extensions
- `rust-analyzer`: Official Rust language server
- `CodeLLDB`: Debugging support
- `Even Better TOML`: Configuration files
- `Git Graph`: Git visualization

### Environment Variables

```bash
# .env file for development
export RUST_LOG=lightning_db=debug
export RUST_BACKTRACE=1
export LIGHTNING_DB_TEST_DIR=/tmp/lightning_db_tests
export CARGO_TARGET_DIR=target

# Performance testing
export RUST_LOG=lightning_db=info
export CARGO_PROFILE_RELEASE_DEBUG=true
```

### Docker Development Environment

```dockerfile
# Dockerfile.dev
FROM rust:1.70

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .

RUN cargo build
CMD ["cargo", "test"]
```

```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  lightning-db-dev:
    build:
      context: .
      dockerfile: Dockerfile.dev
    volumes:
      - .:/workspace
      - cargo-cache:/usr/local/cargo/registry
    environment:
      - RUST_LOG=debug
volumes:
  cargo-cache:
```

## Project Structure

### Directory Layout

```
lightning_db/
├── src/                          # Main source code
│   ├── lib.rs                   # Main library entry point
│   ├── async_database.rs        # Async database interface
│   ├── btree/                   # B+Tree implementation
│   │   ├── mod.rs              # B+Tree main module
│   │   ├── node.rs             # Node structure
│   │   ├── iterator.rs         # Tree iteration
│   │   └── delete.rs           # Deletion operations
│   ├── cache/                   # Caching system
│   │   ├── mod.rs              # Cache interfaces
│   │   ├── arc_cache.rs        # ARC cache implementation
│   │   └── memory_pool.rs      # Memory management
│   ├── compression/             # Compression algorithms
│   ├── lsm/                     # LSM tree implementation
│   ├── storage/                 # Storage engine
│   ├── transaction/             # Transaction management
│   ├── wal/                     # Write-ahead logging
│   └── bin/                     # Binary executables
│       ├── lightning-cli.rs    # CLI tool
│       └── lightning-admin-server.rs
├── examples/                     # Example code
├── benches/                     # Benchmarks
├── tests/                       # Integration tests
├── docs/                        # Documentation
├── proto/                       # Protobuf definitions
├── Cargo.toml                   # Project configuration
├── build.rs                     # Build script
└── README.md
```

### Module Organization

#### Core Modules
- **`lib.rs`**: Main database interface and public API
- **`error.rs`**: Error types and handling
- **`storage/`**: Page management and persistence
- **`btree/`**: B+Tree index structure
- **`lsm/`**: LSM tree for write optimization
- **`transaction/`**: MVCC transaction system
- **`cache/`**: Memory caching and management

#### Supporting Modules
- **`compression/`**: Data compression algorithms
- **`wal/`**: Write-ahead logging for durability
- **`monitoring/`**: Metrics and observability
- **`integrity/`**: Data integrity verification
- **`utils/`**: Utility functions and helpers

### Code Organization Principles

1. **Separation of Concerns**: Each module has a single responsibility
2. **Layered Architecture**: Clear boundaries between layers
3. **Dependency Injection**: Use traits for testability
4. **Error Propagation**: Consistent error handling throughout
5. **Resource Management**: RAII for cleanup and safety

## Building & Testing

### Build Commands

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Build with all features
cargo build --all-features

# Build specific binary
cargo build --bin lightning-cli

# Build with verbose output
cargo build -v
```

### Testing

#### Unit Tests
```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_btree_operations

# Run tests in specific module
cargo test btree::tests

# Run tests with filtering
cargo test --lib integration
```

#### Integration Tests
```bash
# Run integration tests
cargo test --test integration

# Run specific integration test
cargo test --test integration test_database_lifecycle
```

#### Property-Based Tests
```bash
# Run property tests (using proptest)
cargo test --test property_tests

# Run with more test cases
PROPTEST_CASES=10000 cargo test property_tests
```

### Continuous Testing

```bash
# Watch for changes and run tests
cargo watch -x test

# Watch and run specific tests
cargo watch -x "test btree"

# Watch and run with clear screen
cargo watch -c -x test
```

### Performance Testing

```bash
# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench btree_insert

# Profile with flamegraph
cargo flamegraph --example final_benchmark

# Memory profiling with valgrind
cargo build && valgrind --tool=massif ./target/debug/examples/memory_test
```

## Contributing Guidelines

### Workflow

1. **Fork** the repository
2. **Create** feature branch (`git checkout -b feature/new-feature`)
3. **Implement** changes with tests
4. **Commit** with clear messages
5. **Push** to your fork
6. **Submit** pull request

### Commit Message Format

```
type(scope): brief description

Detailed explanation of the change, why it's needed,
and how it works.

Fixes #123
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `perf`: Performance improvement
- `refactor`: Code refactoring
- `test`: Test additions/improvements
- `docs`: Documentation updates
- `style`: Code style changes
- `chore`: Maintenance tasks

**Examples:**
```bash
feat(btree): implement node splitting optimization

Improve B+Tree performance by optimizing node splitting
algorithm to reduce memory allocations.

- Add bulk splitting for multiple keys
- Implement SIMD comparisons
- Reduce memory fragmentation

Closes #456

fix(wal): prevent data loss during recovery

Fix race condition in WAL recovery that could lead to
data loss when multiple transactions commit simultaneously.

Fixes #789
```

### Pull Request Guidelines

#### PR Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Refactoring
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Performance tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or documented)
```

#### Review Criteria
- **Functionality**: Code works correctly
- **Performance**: No significant regressions
- **Safety**: Memory safety and error handling
- **Style**: Follows project conventions
- **Tests**: Adequate test coverage
- **Documentation**: Clear and accurate

## Code Standards

### Rust Style Guidelines

```rust
// Use clear, descriptive names
fn insert_key_value_pair(key: &[u8], value: &[u8]) -> Result<()> {
    // Implementation
}

// Prefer Result<T> over Option<T> for fallible operations
fn get_page(page_id: u32) -> Result<Page, Error> {
    // Implementation
}

// Use type aliases for complex types
type PageId = u32;
type TransactionId = u64;

// Document public APIs
/// Inserts a key-value pair into the database.
/// 
/// # Arguments
/// * `key` - The key to insert
/// * `value` - The value to associate with the key
/// 
/// # Returns
/// * `Ok(())` on success
/// * `Err(Error)` if insertion fails
/// 
/// # Examples
/// ```
/// db.put(b"key", b"value")?;
/// ```
pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    // Implementation
}
```

### Error Handling

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("Data corruption: {0}")]
    Corruption(String),
    
    #[error("Generic error: {0}")]
    Generic(String),
}

// Use ? operator for error propagation
fn read_page(page_id: u32) -> Result<Page> {
    let data = self.page_manager.read_page(page_id)?;
    let page = Page::deserialize(&data)?;
    Ok(page)
}

// Provide context for errors
fn open_database(path: &Path) -> Result<Database> {
    std::fs::File::open(path)
        .map_err(|e| Error::Io(e))
        .with_context(|| format!("Failed to open database at {:?}", path))?;
    // ...
}
```

### Performance Guidelines

```rust
// Avoid allocations in hot paths
fn compare_keys_simd(key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
    // Use SIMD for performance-critical comparisons
    #[cfg(target_feature = "avx2")]
    {
        unsafe { compare_keys_avx2(key1, key2) }
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        key1.cmp(key2)
    }
}

// Use pre-allocated buffers
struct WriteBuffer {
    buffer: Vec<u8>,
}

impl WriteBuffer {
    fn write_entry(&mut self, key: &[u8], value: &[u8]) {
        self.buffer.clear(); // Reuse allocation
        self.buffer.extend_from_slice(key);
        self.buffer.extend_from_slice(value);
    }
}

// Minimize lock scope
fn update_statistics(&self, operation: OperationType, duration: Duration) {
    let stats = {
        let mut stats = self.stats.lock();
        stats.update(operation, duration);
        stats.clone() // Clone only necessary data
    }; // Lock released here
    
    // Process stats without holding lock
    self.process_stats(stats);
}
```

### Testing Standards

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    // Test naming: test_<module>_<scenario>_<expected_behavior>
    #[test]
    fn test_btree_insert_new_key_succeeds() {
        let dir = tempdir().unwrap();
        let btree = BTree::new(dir.path()).unwrap();
        
        // Arrange
        let key = b"test_key";
        let value = b"test_value";
        
        // Act
        let result = btree.insert(key, value);
        
        // Assert
        assert!(result.is_ok());
        assert_eq!(btree.get(key).unwrap(), Some(value.to_vec()));
    }
    
    // Test error conditions
    #[test]
    fn test_btree_insert_duplicate_key_returns_error() {
        let dir = tempdir().unwrap();
        let btree = BTree::new(dir.path()).unwrap();
        
        btree.insert(b"key", b"value1").unwrap();
        let result = btree.insert(b"key", b"value2");
        
        assert!(matches!(result, Err(Error::DuplicateKey(_))));
    }
    
    // Property-based tests
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn test_btree_roundtrip(
            key in prop::collection::vec(any::<u8>(), 1..1000),
            value in prop::collection::vec(any::<u8>(), 1..1000)
        ) {
            let dir = tempdir().unwrap();
            let btree = BTree::new(dir.path()).unwrap();
            
            btree.insert(&key, &value)?;
            let retrieved = btree.get(&key)?;
            
            prop_assert_eq!(retrieved, Some(value));
        }
    }
}
```

## Architecture Guidelines

### Design Principles

#### Database-Specific Considerations
```rust
// CRITICAL: This is a DATABASE - every design decision must prioritize:
// 1. Data integrity - never lose or corrupt data
// 2. Reliability - consistent behavior under all conditions
// 3. Durability - survive crashes and recover fully
// 4. Performance - meet SLAs consistently

// Example: Always validate data integrity
fn write_page(&mut self, page_id: u32, data: &[u8]) -> Result<()> {
    // 1. Validate data before writing
    self.validate_page_data(data)?;
    
    // 2. Write to WAL first for durability
    self.wal.log_page_write(page_id, data)?;
    
    // 3. Update in-memory structure
    self.update_page_cache(page_id, data)?;
    
    // 4. Schedule async write to disk
    self.schedule_page_write(page_id, data)?;
    
    // 5. Verify write succeeded
    self.verify_page_write(page_id)?;
    
    Ok(())
}
```

#### Layered Architecture

```rust
// Application Layer
pub struct Database {
    // Storage Engine Layer
    storage_engine: StorageEngine,
    
    // Transaction Layer
    transaction_manager: TransactionManager,
    
    // Caching Layer  
    cache_manager: CacheManager,
}

// Each layer has clear interfaces
pub trait StorageEngine {
    fn read_page(&self, page_id: PageId) -> Result<Page>;
    fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()>;
    fn allocate_page(&mut self) -> Result<PageId>;
    fn deallocate_page(&mut self, page_id: PageId) -> Result<()>;
}

pub trait TransactionManager {
    fn begin(&mut self) -> Result<TransactionId>;
    fn commit(&mut self, tx_id: TransactionId) -> Result<()>;
    fn abort(&mut self, tx_id: TransactionId) -> Result<()>;
}
```

#### Concurrency Design

```rust
// Use fine-grained locking
pub struct BTree {
    // Reader-writer lock for tree structure
    root: RwLock<NodeId>,
    
    // Per-node locks to minimize contention
    node_locks: DashMap<NodeId, RwLock<Node>>,
    
    // Lock-free operations where possible
    statistics: AtomicStatistics,
}

// Lock ordering to prevent deadlocks
impl BTree {
    fn split_node(&self, node_id: NodeId) -> Result<()> {
        // Always acquire locks in consistent order
        let _tree_lock = self.root.write();
        let _node_lock = self.get_node_lock(node_id).write();
        
        // Perform split operation
        self.perform_split(node_id)
    }
}
```

### Error Handling Architecture

```rust
// Hierarchical error types
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Storage error")]
    Storage(#[from] StorageError),
    
    #[error("Transaction error")]
    Transaction(#[from] TransactionError),
    
    #[error("Cache error")]
    Cache(#[from] CacheError),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Page not found: {page_id}")]
    PageNotFound { page_id: u32 },
    
    #[error("I/O error: {source}")]
    Io { #[from] source: std::io::Error },
    
    #[error("Corruption detected: {details}")]
    Corruption { details: String },
}

// Error recovery strategies
impl Database {
    fn handle_corruption_error(&self, error: &StorageError) -> Result<()> {
        match error {
            StorageError::Corruption { details } => {
                // Log corruption event
                tracing::error!("Data corruption detected: {}", details);
                
                // Attempt automatic repair
                if let Err(repair_error) = self.attempt_repair() {
                    tracing::error!("Repair failed: {}", repair_error);
                    
                    // Escalate to manual intervention
                    return Err(DatabaseError::Storage(StorageError::Corruption {
                        details: format!("Automatic repair failed: {}", repair_error)
                    }));
                }
                
                Ok(())
            },
            _ => Err(DatabaseError::Storage(error.clone())),
        }
    }
}
```

## Performance Optimization

### Profiling Tools

```bash
# CPU profiling with flamegraph
cargo flamegraph --example benchmark

# Memory profiling
cargo run --example memory_test
valgrind --tool=massif ./target/debug/examples/memory_test

# Benchmark with criterion
cargo bench --bench btree_benchmark

# Profile-guided optimization
RUSTFLAGS="-Cprofile-generate=/tmp/pgo-data" cargo build --release
# Run workload
RUSTFLAGS="-Cprofile-use=/tmp/pgo-data" cargo build --release
```

### Hot Path Optimization

```rust
// Identify hot paths with profiling
#[inline(always)]
fn compare_keys_hot_path(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    // Hand-optimized for common case
    if a.len() != b.len() {
        return a.len().cmp(&b.len());
    }
    
    // Use SIMD when available
    #[cfg(target_feature = "avx2")]
    unsafe {
        return compare_simd_avx2(a, b);
    }
    
    // Fallback to standard comparison
    a.cmp(b)
}

// Cache-friendly data structures
#[repr(C)]
struct CacheFriendlyNode {
    // Pack related data together
    key_count: u16,
    is_leaf: bool,
    _padding: u8, // Align to word boundary
    
    // Keep keys and values separate for better cache usage
    keys: [u8; 4096 - 8], // Fill cache line
}

// Minimize allocations
fn batch_insert(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
    // Pre-allocate batch buffer
    let mut batch_buffer = Vec::with_capacity(entries.len() * 64);
    
    for (key, value) in entries {
        // Reuse allocation
        batch_buffer.clear();
        batch_buffer.extend_from_slice(key);
        batch_buffer.extend_from_slice(value);
        
        self.insert_from_buffer(&batch_buffer)?;
    }
    
    Ok(())
}
```

### Memory Optimization

```rust
// Use memory pools for frequent allocations
pub struct MemoryPool {
    page_pool: Vec<Box<[u8; 4096]>>,
    buffer_pool: Vec<Vec<u8>>,
}

impl MemoryPool {
    fn get_page(&mut self) -> Box<[u8; 4096]> {
        self.page_pool.pop()
            .unwrap_or_else(|| Box::new([0; 4096]))
    }
    
    fn return_page(&mut self, page: Box<[u8; 4096]>) {
        if self.page_pool.len() < 1000 { // Limit pool size
            self.page_pool.push(page);
        }
    }
}

// Use Cow for copy-on-write optimization
use std::borrow::Cow;

fn get_key<'a>(&'a self, index: usize) -> Cow<'a, [u8]> {
    if self.keys_compressed {
        Cow::Owned(self.decompress_key(index))
    } else {
        Cow::Borrowed(&self.keys[index])
    }
}
```

## Debugging & Profiling

### Debugging Tools

```bash
# Debug build with symbols
cargo build --debug

# Run with debugger
rust-gdb ./target/debug/lightning_db
# or
rust-lldb ./target/debug/lightning_db

# Enable debug logs
RUST_LOG=lightning_db=debug cargo run --example debug_test

# Memory debugging
RUST_BACKTRACE=full cargo test test_that_crashes

# Sanitizers
RUSTFLAGS="-Z sanitizer=address" cargo test
RUSTFLAGS="-Z sanitizer=memory" cargo test
```

### Debug Utilities

```rust
// Debug printing with conditional compilation
#[cfg(debug_assertions)]
macro_rules! debug_print {
    ($($arg:tt)*) => {
        eprintln!("[DEBUG] {}: {}", module_path!(), format!($($arg)*));
    };
}

#[cfg(not(debug_assertions))]
macro_rules! debug_print {
    ($($arg:tt)*) => {};
}

// Runtime debugging features
impl Database {
    #[cfg(feature = "debug")]
    pub fn debug_dump_state(&self) -> String {
        format!("Database state:\n\
                 - Pages: {}\n\
                 - Transactions: {}\n\
                 - Cache: {}\n",
                self.storage.page_count(),
                self.transaction_manager.active_count(),
                self.cache.hit_rate())
    }
    
    #[cfg(feature = "debug")]
    pub fn debug_validate_invariants(&self) -> Result<()> {
        // Expensive validation only in debug builds
        self.btree.validate_tree_structure()?;
        self.transaction_manager.validate_consistency()?;
        Ok(())
    }
}
```

### Performance Debugging

```rust
// Instrumentation for hot paths
use std::time::Instant;

struct PerformanceTimer {
    name: &'static str,
    start: Instant,
}

impl PerformanceTimer {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
        }
    }
}

impl Drop for PerformanceTimer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        if duration > Duration::from_millis(1) {
            tracing::warn!("Slow operation {}: {:?}", self.name, duration);
        }
    }
}

// Usage in hot paths
fn expensive_operation(&self) -> Result<()> {
    let _timer = PerformanceTimer::new("expensive_operation");
    // Operation implementation
    Ok(())
}
```

## Release Process

### Version Management

```bash
# Update version in Cargo.toml
cargo install cargo-edit
cargo set-version 0.2.0

# Create release branch
git checkout -b release/0.2.0

# Update changelog
echo "## [0.2.0] - $(date +%Y-%m-%d)" >> CHANGELOG.md
```

### Pre-Release Checklist

```bash
#!/bin/bash
# scripts/pre-release.sh

set -e

echo "Running pre-release checks..."

# 1. Code quality
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings

# 2. Security audit
cargo audit
cargo deny check

# 3. Test suite
cargo test --all-features
cargo test --release

# 4. Benchmarks (ensure no regressions)
cargo bench -- --baseline previous

# 5. Documentation
cargo doc --all-features --no-deps

# 6. Integration tests
cargo test --test integration

# 7. Examples
for example in examples/*.rs; do
    cargo run --example $(basename $example .rs)
done

echo "All checks passed!"
```

### Release Build

```bash
# Optimized release build
RUSTFLAGS="-C target-cpu=native -C lto=fat" cargo build --release

# Cross-compilation for different targets
cargo install cross
cross build --target x86_64-unknown-linux-gnu --release
cross build --target x86_64-pc-windows-gnu --release
cross build --target x86_64-apple-darwin --release

# Create release artifacts
mkdir -p dist/
cp target/release/lightning-cli dist/
cp target/release/lightning-admin-server dist/
tar -czf lightning-db-v0.2.0-linux-x86_64.tar.gz -C dist/ .
```

### Documentation Updates

```bash
# Generate API documentation
cargo doc --all-features --no-deps

# Update README with new features
# Update CHANGELOG.md
# Update docs/ directory

# Publish documentation
cargo doc --all-features --no-deps --open
```

---

This development guide provides comprehensive information for contributing to Lightning DB. It emphasizes the critical nature of database development where data integrity and reliability are paramount.

Remember: **This is a DATABASE** - every change must be carefully considered and thoroughly tested. Data loss is never acceptable.

*Last Updated: Development Guide v1.0*  
*Next Review: After major development process changes*  
*Owner: Lightning DB Development Team*