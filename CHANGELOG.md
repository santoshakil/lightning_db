# Changelog

All notable changes to Lightning DB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive real-world test suite covering:
  - E-commerce scenarios with transactions
  - Time series data handling
  - Concurrent operations stress testing
  - Crash recovery simulation
  - Large value handling
  - Batch operations performance
- Quick performance test utility for benchmarking

### Changed
- Optimized transaction logging from info to debug level for 15x performance improvement
- Updated performance benchmarks in documentation:
  - Sequential writes: 296K+ ops/sec
  - Cached reads: 255K+ ops/sec
  - Batch operations: 2.3M+ ops/sec
  - Transactions: 71K+ ops/sec

### Fixed
- Critical unreachable!() panic in get_cache_stats method
- B-tree node splits test with incorrect reverse insertion logic
- Duplicate dependencies in Cargo.toml (hex, flate2, subtle)

### Removed
- Test module from lib.rs (315 lines) - tests now properly separated
- .DS_Store files from repository
- Dead code and commented-out implementations

## [0.1.0] - 2024-09-12

### Core Features
- B+ Tree indexing for fast lookups
- LSM Tree for write optimization
- MVCC transactions with multiple isolation levels
- Write-ahead logging for durability
- Adaptive caching with ARC algorithm
- Compression support (LZ4, Snappy, Zstd)
- Encryption at rest (AES-256-GCM)
- FFI bindings for C/C++ integration
- Cross-platform support (Linux, macOS, Windows)

### Performance
- Sub-microsecond read latency
- Millions of operations per second
- Lock-free operations on critical paths
- Memory-mapped I/O support
- Configurable cache and memory usage

### Reliability
- ACID compliance
- Automatic crash recovery
- Data integrity with CRC32 checksums
- Deadlock detection and resolution
- Point-in-time recovery
- Online backup support

### Security
- Input validation and sanitization
- Resource limits and quotas
- Secure defaults
- No unsafe code in core modules
- Memory safety guaranteed by Rust