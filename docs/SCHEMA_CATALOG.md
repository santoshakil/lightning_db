# Schema & Index Catalog

Date: 2025-09-01
Owners: Schema Auditor, Docs & Runbooks

Scope
- Catalog of internal storage, index types, and migration constructs for Lightning DB. Foreign keys are not modeled (embedded KV), so FK integrity is N/A by design. Primary/unique invariants are enforced via index logic.

Components
- B+Tree (core::btree): primary structure for point lookups and ordered scans.
  - Pages: fixed-size; integrity verified by page-level checksums.
  - Node ops: split/merge with correctness tests; write buffer optional.
- LSM Tree (core::lsm): optional write-optimized path with compaction.
  - Compression: Snappy/LZ4/Zstd (feature-dependent), level configurable in config.
- WAL (core::wal): Basic and Unified WAL implementations.
  - Modes: Sync, Async, Periodic; group-commit configurable.
  - Recovery: replays committed operations; integrates with transaction recovery state.
- Index Manager (core::index): secondary indexes and join utilities.
  - Types: `BTree`-like secondary indexes; bitmap indexes optional (feature `bitmap-indexes`).
  - Queries: equality, range, multi-index, join (nested-loop/planned).
- Transactions (features::transactions): unified transaction manager with isolation controls.
  - Isolation levels: ReadCommitted, RepeatableRead, Serializable; deadlock detection optional.
  - Range/point locks; validation and cleanup metrics.
- Prefetch & Cache (performance::{prefetch,cache}): ARC-like unified cache; prefetch manager optional.
- Compression/Encryption (features): value/page compression; encryption manager optional.

Migration System (features::migration)
- Artifacts: versioned `Migration` with metadata (type, mode, dependencies, checksum).
- Managers: `SchemaManager`, `HistoryManager`, `MigrationManager` (register, validate, execute, rollback).
- Templates: create table/index/data migrations; generator and CLI helpers.
- Execution: dry-run, batch size, timeout; progress tracking; rollback plan generator.

Constraints & Integrity
- Primary/unique: enforced by index uniqueness checks; documented in index manager.
- Foreign keys: N/A (embedded KV); higher-level FK semantics to be implemented by consumers if needed.
- Checksums: page-level and optional data checksums; integrity checker traverses structures and can sample checks.

Operational Notes
- Snapshots: `scripts/create_snapshot.sh` seeds KVs and runs compressed backup.
- Integrity: `lightning-cli check` supports checksum sampling and summary/detailed output.
- Backups/Restore: `lightning-cli backup` / `restore`.

Testing & Validation
- Integration tests (feature `integration_tests`) cover isolation, recovery, performance, concurrency safety.
- Migration example (`examples/migration_example.rs`) demonstrates generator/validator/runner flow.

Limitations
- No FK semantics; no external SQL layer; analytics are KV-scan and vectorized operator readiness.
- Some modules use `unsafe` for performance—audited with clear rationale.

