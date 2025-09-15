# Changelog

## [Unreleased] - 2025-09-16

### Added
- Comprehensive test suite with integration, performance, and stress tests
- Test results documentation (TEST_RESULTS.md)
- Database module split into logical submodules (operations, transactions, indexing, maintenance, metrics, lifecycle)
- Quick performance test utility for benchmarking

### Changed
- Consolidated 50+ error types into ~30 core error types
- Simplified error handling throughout codebase
- Cleaned up 200+ compilation warnings
- Simplified README documentation to be more concise
- Fixed test compilation errors and imports
- Optimized transaction logging from info to debug level for 15x performance improvement
- Refactored Database implementation into separate modules for better maintainability

### Fixed
- Cache stats access with proper AtomicUsize operations
- Test imports and common module usage
- B+Tree split handler method references
- Range scan parameter types in tests
- Critical unreachable!() panic in get_cache_stats method
- B-tree node splits test with incorrect reverse insertion logic
- Compilation errors from modularization
- All import warnings and unused code warnings

### Removed
- Broken example file (new_api_demo.rs)
- 50+ redundant error types
- Excessive logging from hot paths (put, get, delete operations)
- Test module from lib.rs (315 lines) - tests now properly separated
- Dead code and commented-out implementations

### Known Issues
- Batch operations returning incorrect results in some tests (4/10 integration tests failing)
- B+Tree range scan iterator not fully implemented
- Iterator consistency issues
- LSM flush showing empty memtable rotations

## [0.1.0] - Previous Release

### Core Features
- B+ Tree and LSM Tree storage engines
- ACID transactions with MVCC
- Write-ahead logging
- Page-based storage with checksums
- Configurable caching and compression
- Cross-platform support (Linux, macOS, Windows)