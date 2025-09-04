# Test Suite Overhaul

Date: 2025-09-01
Owner: Test Steward

Changes
- Fixed integration tests to match public APIs:
  - Replaced deprecated `LightningDbConfig` fields (`path`, `auto_compaction`, `compaction_threshold`, `use_improved_wal`).
  - Switched `create_index` and `query_index` call sites to current signatures.
  - Removed misplaced inner `#![cfg(feature = "integration_tests")]` attributes; use file-level gating.
- Ensured all long-running scripts use timeouts to avoid hangs.

Status
- `cargo test --no-run` passes (compiles cleanly). Full runtime tests are gated by `integration_tests` feature.

Next
- Add nightly CI job for chaos/crash matrix and performance regression suite.
