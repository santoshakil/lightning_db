# Enterprise Hardening Plan (V2)

Date: 2025-09-01
Role: Principal Database Reliability Engineer
Scope: Harden Lightning DB for OLTP and OLAP with zero new features; improve stability, maintainability, and reproducibility.

## Governance & Guardrails
- No new product features. No external behavior changes unless an ADR documents it and gets explicit approval.
- Deterministic, reproducible workflows: pinned toolchain, scripts, and configs; all changes via ADRs.
- Safety-first operations: snapshots/backups before risky changes; tested rollback.
- Transparency: every substantive change includes an ADR, benchmarks/tests, evidence artifacts.

## Sub-Agents (Owners)
- Schema Auditor: schema/migration validity, index/constraint audit, internal catalog docs.
- Query Optimizer: slow query patterns, concurrency/lock analysis, OLTP index strategy.
- OLAP Architect: columnar storage readiness, partitioning, MVs/refresh processes, stats maintenance.
- Data Quality & Migrations: data scans, repair scripts, forward/backward migrations, idempotency.
- Test Steward: test triage, realistic fixtures, perf/regression harness; coverage and flake control.
- Docs & Runbooks: architecture, schema catalog, ops playbooks, backup/DR, tuning and analytics guides.
- Risk Officer: backups, rollback drills, RTO/RPO validation, incident response rehearsals.
- PM/Coordinator: phase sequencing, acceptance criteria, evidence collection, sign-off gatekeeping.

## Phases, Tasks, Dependencies, Acceptance, ETA

### Phase 0 — Discover & Baseline
Owner: PM/Coordinator (support: all)
Dependencies: none
ETA: 0.5–1 day
Acceptance Criteria:
- Complete repo inventory (code, tests, benches, scripts, docs) and toolchain pin verified.
- Snapshots created under `snapshots/<date>/` with seeded KVs and compressed backup.
- Baselines captured: OLTP quick baseline; OLAP KV-scan baseline; artifacts in `benchmark_results/<date>/`.
- Schema/index catalog drafted from current engine and migration framework.

Tasks:
- Inventory: modules, features, benches, tests, scripts; map entrypoints and binary tooling.
- Tooling check: rust-toolchain version, `Cargo.lock`, linters, security scans; confirm `pre-commit` hooks.
- Create snapshot: `scripts/create_snapshot.sh` (deterministic seeds, timeouts) and store outputs.
- Run quick baselines: `scripts/baseline_oltp.sh`, `scripts/baseline_olap_kv.sh`.
- Draft `docs/SCHEMA_CATALOG.md` from migration engine, index manager, and storage structures; note FK semantics are N/A.

Evidence:
- `snapshots/<date>/` created; `benchmark_results/<date>/` JSON + text outputs.
- `docs/SCHEMA_CATALOG.md` committed.

Risks/Mitigations:
- Build breaks: compile with `cargo test --no-run` first; fix minimal blockers only.
- Long benches: use quick mode initially; defer full suite until after stabilization pass.

---

### Phase 1 — Plan & ADRs
Owner: PM/Coordinator
Dependencies: Phase 0
ETA: 0.5 day
Acceptance Criteria:
- This plan committed as V2; owners and acceptance criteria agreed.
- ADR created to align governance/toolchain to actual pinned version.

Tasks:
- Finalize and commit V2 plan.
- ADR 0003: toolchain alignment (match `rust-toolchain.toml`), CI note, rollback steps.

Evidence:
- `docs/ENTERPRISE_HARDENING_PLAN_V2.md`; `docs/adr/0003-toolchain-alignment.md`.

---

### Phase 2 — Stability Pass (Schema, Migrations, Integrity)
Owner: Schema Auditor + Data Quality & Migrations + Risk Officer
Dependencies: Phase 1
ETA: 2–3 days
Acceptance Criteria:
- Migration engine audit: idempotent, deterministic ordering; checksum validation enforced in tests; rollback simulation passes.
- Index/constraint correctness: no orphaned entries; unique/primary invariants enforced by tests; FK noted as N/A.
- WAL/recovery correctness: replay paths validated for unified and standard WAL modes; recovery tests green.
- Data integrity: integrity checker passes on fresh + restored snapshots; page checksum sampling documented.

Tasks:
- Audit `src/features/migration/*` and `templates.rs`; ensure validator covers up/down/checksum and dry-run.
- Verify `SchemaManager` current version handling; add tests if missing (no feature changes).
- Index audit: scan IndexManager for orphan risks; add targeted tests for delete/rename flows.
- Recovery: run recovery tests; chaos injectors (fsync reorder, crash loops) in integration harness.
- Docs: update `docs/SCHEMA_INTEGRITY_REPORT.md` with findings/evidence and clear scope for FK support.

Evidence:
- Updated integrity report; test results and logs.

Risks/Mitigations:
- Recovery flakiness: increase determinism via seeded operations and timeouts.
- Migration rollbacks: validate dry-run and rollback on snapshot restore.

---

### Phase 3 — OLTP Performance Pass
Owner: Query Optimizer (+ Schema Auditor)
Dependencies: Phase 2
ETA: 3–4 days
Acceptance Criteria:
- Latency/throughput SLOs met or improved: ≥20% p95 improvement or ≥20% TPS improvement vs baseline, or documented justification.
- Lock contention analysis with measurable reduction ≥30% in mixed workload tests.
- Isolation semantics verified: repeatable read, serializable paths covered; deadlock detection/response <100ms.

Tasks:
- Profile hot paths via benches; capture flamegraphs and lock wait stats.
- Index strategy: confirm read/write balance; ensure covering/secondary indexes align with OLTP tests.
- Tune cache/page manager settings; confirm page hit ratio ≥90% on read-heavy run with configured cache.
- Document knobs/tunings; no external API changes.

Evidence:
- `benchmark_results/<date>/` reports; updated `OLTP_PERFORMANCE_BASELINE.md` with before/after.

Risks/Mitigations:
- Regressions: `scripts/run_regression_benchmarks.sh` with 5–10% auto-fail threshold in quick checks.

---

### Phase 4 — Analytics/Columnar Readiness
Owner: OLAP Architect (+ Query Optimizer)
Dependencies: Phase 3
ETA: 3–4 days
Acceptance Criteria:
- Columnar formats, segment/page types validated; vectorized operators and predicate pushdown validated via benches.
- Stats maintenance hooks exercised; MV refresh flows scheduled via tests (no feature additions; validate existing paths where present).
- Partitioning and scan strategies documented with example workloads and measured performance.

Tasks:
- Validate compression/encoding choices (dictionary, RLE, delta) with representative datasets; report compression ≥5x.
- Validate late materialization and vectorized aggregates at batch size ≥1024.
- Document columnar usage guide and limits; note any deferred areas via ADR if needed.

Evidence:
- Bench and compression metrics, analytics usage guide.

Risks/Mitigations:
- CPU variability: pin CPU governor (where allowed) during benches; run 3x and average.

---

### Phase 5 — Test Suite Overhaul
Owner: Test Steward
Dependencies: Phase 4
ETA: 2–3 days
Acceptance Criteria:
- Zero failing tests; CI green; no warnings.
- Realistic data volumes used in integration/perf tests; suite ≤5 minutes locally (fast path) and meets CI SLA.
- Clear categorization: unit/integration/perf; flakiness eliminated (timeouts, nondeterminism removed).

Tasks:
- Remove/disable dead tests referencing deprecated configs; modernize to current APIs without changing external behavior.
- Ensure feature flags gate integration suites (e.g., `integration_tests`).
- Expand perf regression harness with quick mode gate in CI; nightly full mode retained.

Evidence:
- CI logs, coverage and category mapping, updated `tests/README.md`.

Risks/Mitigations:
- Intermittent failures: add retries only for infra steps, not assertions; tighten timeouts deterministically.

---

### Phase 6 — Documentation & Runbooks
Owner: Docs & Runbooks (+ Risk Officer)
Dependencies: Phase 5
ETA: 2–3 days
Acceptance Criteria:
- Architecture docs reflect current code; schema catalog complete; on-disk format described.
- Ops runbooks: backup/restore/DR, incident response, performance tuning, analytics usage.
- ADRs and changelogs up to date.

Tasks:
- Update top-level README; module architecture; schema catalog; migration catalog.
- Ops: backup/restore, snapshots, RTO/RPO validation, monitoring.
- Guides: OLTP tuning, analytics usage; perf troubleshooting.

Evidence:
- Updated docs in `docs/`; generated diagrams if applicable.

---

### Phase 7 — Repository Cleanup
Owner: PM/Coordinator
Dependencies: Phase 6
ETA: 1–2 days
Acceptance Criteria:
- Unused code/tests/docs removed or archived under `/archive`.
- Lint/format/security checks enforced; `.gitignore` covers derived artifacts.
- Toolchain and configs consolidated; one source of truth in docs.

Tasks:
- Archive stale reports into `archive/`; retain essential summaries.
- Remove unused examples or update to current APIs; prune dead configs.
- Enforce clippy/rustfmt with `-D warnings` where feasible.

Evidence:
- PR with deletions/moves; CI lint passes.

---

### Phase 8 — Final Validation & Sign-off
Owner: Risk Officer + PM/Coordinator
Dependencies: Phases 0–7
ETA: 1–2 days
Acceptance Criteria:
- Before/after metrics documented; OLTP/OLAP targets met or justified.
- Backups/restores verified end-to-end; documented RTO/RPO achieved.
- Zero failing tests; zero warnings; no orphaned migrations or unused artifacts.
- Sign-off report published with risk register and next maintenance window.

Tasks:
- Run full test/perf suites; compare against baseline; attach artifacts.
- Validate rollback on a staging snapshot.
- Publish `docs/FINAL_SIGN_OFF_REPORT.md` update.

Evidence:
- Final report, artifacts, and ADR references.

## Cross-Cutting Acceptance Thresholds
- OLTP: ≥20% p95 latency improvement or ≥20% TPS improvement vs Phase 0 baseline, or written justification.
- OLAP: measurable scan/aggregate speedups on target datasets; stable MV/refresh scheduling where applicable (no new features).
- Stability: Zero failing tests; zero warnings; integrity checks pass; recovery validated.
- Operations: Documented, tested backups/restore; RTO/RPO achieved.

## Risk Register (Selected)
- Build/toolchain drift: Align ADRs to pinned toolchain, run `cargo +toolchain` in CI.
- Perf regressions: Use quick regression gates (fail ≥5–10%); nightly full runs; rollback to last known good.
- Data integrity: Always snapshot before risky tasks; dry-run migrations; verify with integrity checker.
- Test flakiness: deterministic seeds; remove sleeps; control timeouts.

## Execution Notes
- Sequence risky steps last; always snapshot before touching durability/recovery internals.
- All measurable changes include: proposal → ADR → change/patch → benchmarks/tests → evidence summary.

