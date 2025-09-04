# Analytics Usage Guide (KV)

Date: 2025-09-01
Owners: OLAP Architect, Docs & Runbooks

Scope
- Practical guidance for running analytical-style scans and aggregates on Lightning DB’s KV engine paths. This guide validates columnar/vectorized readiness where available and documents current limits.

Quick Start (KV Scan)
- Example: `examples/olap_kv_scan.rs`
- Run baseline: `scripts/baseline_olap_kv.sh`
- Tunables (via `LightningDbConfig`):
  - `compression_enabled`: enable to use LSM compression (Snappy/LZ4 depending on features)
  - `cache_size`: size in bytes; larger can help repeated scans with hot segments
  - `wal_sync_mode`: Async for ingest speed; Sync for minimal RPO

Recommended Patterns
- Bulk load, then range-scan with tight key prefixes.
- Use sequential key layouts for high locality (`scan_0000000000..scan_000NNNNNNNN`).
- Batch application-side aggregation in chunks of 1024+ rows to benefit from vectorized operations where present.

Maintenance
- Periodic compaction recommended for long-lived datasets; verify with `lightning-cli compact` if exposed.
- Run `lightning-cli stats --detailed` to observe cache, WAL, and LSM activity.

Current Limits
- `examples/olap_scan.rs` is an advanced demo and may not compile under current feature flags; use KV path example for baselines.
- Full SQL-like columnar operators are not part of the external contract; analytics are achieved via KV scans and client-side vectorized operations.

Artifacts
- Baselines stored under `benchmark_results/<date>/`.
- See `docs/FINAL_SIGN_OFF_REPORT.md` for before/after metrics.

