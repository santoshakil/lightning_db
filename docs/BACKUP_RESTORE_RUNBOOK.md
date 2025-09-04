# Backup & Restore Runbook

Date: 2025-09-01
Owner: Data Quality & Migrations

Backup
- Full: `cargo run --features cli --bin lightning-backup <db_path> <backup_dir>`
- Incremental (if supported by config): `cargo run --features cli --bin lightning-cli -- backup <db_path> <out> --incremental`

Restore
- `cargo run --features cli --bin lightning-cli -- restore <backup_path> <dest> --verify`
- Validate get/scan on restored DB; run integrity checker.

Verification
- After restore, run a targeted workload and compare counts/hashes.

Safety
- Scripts use timeouts; capture logs and artifacts under `snapshots/<date>/`.

