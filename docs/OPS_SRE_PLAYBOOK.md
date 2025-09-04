# Operations SRE Playbook

Date: 2025-09-01
Owners: Risk Officer + Docs & Runbooks

Runbooks
- Backup: `cargo run --features cli --bin lightning-backup <db_path> <backup_dir>`
- Restore: `cargo run --features cli --bin lightning-cli -- restore <backup_path> <dest>`
- Snapshot: `scripts/create_snapshot.sh` (uses timeouts, seeds data)
- OLTP baseline: `scripts/baseline_oltp.sh`
- OLAP baseline (KV): `scripts/baseline_olap_kv.sh`

RTO/RPO
- With WAL Sync: RPO ~0; Async: RPO <= last flush interval; Periodic: RPO <= interval.
- RTO target: ≤5 minutes on datasets tested (verify on environment hardware).

Incident Response
- WAL Corruption: Stop writes, restore from last backup, reapply WAL segments where valid.
- Crash Loops: Run recovery; if persistent, restore snapshot and roll-forward.

Monitoring
- Use statistics in `features::statistics`; check cache hit, LSM flush, WAL throughput.

Change Management
- Every change requires ADR with evidence and rollback steps.
- Toolchain pinned (ADR 0003); update requires ADR + baselines (`cargo test --no-run`, quick OLTP baseline).
