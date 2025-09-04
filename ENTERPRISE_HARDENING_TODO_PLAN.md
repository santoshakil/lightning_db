# Lightning DB Enterprise Hardening TODO Plan

**Date:** September 1, 2025  
**Version:** 1.0.0  
**Lead:** Principal Database Reliability Engineer  
**Mission:** Harden Lightning DB for enterprise OLTP/OLAP workloads with zero new features

## Phase 1: Foundation & Discovery ✅
**Status:** COMPLETED  
**Owner:** PM/Coordinator  
**Duration:** Day 1  

### Deliverables
- [x] Codebase structure analysis
- [x] Dependency audit  
- [x] Compilation status check
- [x] Test coverage assessment
- [x] Security report review

## Phase 2: Critical Fixes & Stabilization
**Status:** IN PROGRESS  
**Owner:** Risk Officer + Test Steward  
**Duration:** 2–4 days  
**Dependencies:** None  

### 2.1 Fix Build & Test Failures (Blocking)
**Owner:** Test Steward  
**Acceptance Criteria:**
- [ ] Resolve all library compile errors (e.g., borrow issues in encryption tests)
- [ ] Fix or update broken tests referencing stale config (e.g., `use_improved_wal`, `enable_lsm_tree`)
- [ ] Adapt tests to async APIs (e.g., `get_compaction_stats`) deterministically
- [ ] Zero failing tests locally and in CI
- [ ] Release build compiles with `-D warnings` cleanly

### 2.2 Code Hygiene Cleanup
**Owner:** PM/Coordinator  
**Acceptance Criteria:**
- [ ] Zero unused imports, variables, or dead code warnings
- [ ] All TODO/FIXME triaged; critical items ticketed or resolved
- [ ] Dependencies pinned and reduced to <100 core crates
- [ ] Clippy clean under `--all-features -- -D warnings`

### 2.3 Security Pass (No Behavior Changes)
**Owner:** Risk Officer  
**Acceptance Criteria:**
- [ ] All `unwrap/expect` audited and replaced where unsafe
- [ ] All unsafe blocks reviewed and documented with rationale
- [ ] Miri and address sanitizer runs clean for targeted modules
- [ ] Secrets scanning clean; `.secrets.baseline` updated
- [ ] Timing-attack mitigations verified (docs + targeted tests)
- [ ] ADR: “Security posture verification without feature changes”

## Phase 3: OLTP Performance Hardening
**Status:** PENDING  
**Owner:** Query Optimizer + Schema Auditor  
**Duration:** 3–4 days  
**Dependencies:** Phase 2 completion  

### 3.1 Transaction Performance Baseline
**Owner:** Query Optimizer  
**Acceptance Criteria:**
- [ ] Benchmarks for put/get/delete, range scans, batched ops
- [ ] Baseline metrics: p50/p95/p99 latency and TPS under 1, 4, 16, 64 threads
- [ ] Lock/wait graphs captured from internal metrics
- [ ] Deadlock frequency observed and recorded
- [ ] Artifacts: JSON + HTML reports checked into `benchmark_results/`

### 3.2 Lock & Concurrency Optimization
**Owner:** Query Optimizer  
**Acceptance Criteria:**
- [ ] Lock contention reduced by >30% vs baseline on mixed workload
- [ ] Deadlock detection/respond <100ms in synthetic deadlock tests
- [ ] MVCC read-your-writes and repeatable-read validated
- [ ] Isolation-level behavior verified by targeted tests (no phantoms/lost updates)
- [ ] ADR: “Concurrency tuning without external API changes”

### 3.3 B+Tree Performance & Integrity
**Owner:** Schema Auditor  
**Acceptance Criteria:**
- [ ] Node split/merge remain correct under stress; no regressions
- [ ] Page cache hit ratio ≥90% with configured cache in read-heavy runs
- [ ] Crash/recovery chaos tests (power-cut, reorder) pass 100 randomized scenarios
- [ ] Zero data corruption across fsync permutations; artifacts saved
- [ ] ADR: “B+Tree integrity verification scope”

## Phase 4: OLAP/Columnar Optimization
**Status:** PENDING  
**Owner:** OLAP Architect + Query Optimizer  
**Duration:** 3–4 days  
**Dependencies:** Phase 3 completion  

### 4.1 Columnar Engine Validation
**Owner:** OLAP Architect  
**Acceptance Criteria:**
- [ ] Columnar page/segment formats validated with roundtrip tests
- [ ] Compression ratios ≥5x on synthetic + semi-real data
- [ ] Predicate pushdown verified by targeted filter-selectivity benches
- [ ] Stats maintenance routines scheduled via tests (no new features)
- [ ] Null handling covered in vectorized paths

### 4.2 Compression & Encoding
**Owner:** OLAP Architect  
**Acceptance Criteria:**
- [ ] Dictionary/RLE/delta paths benchmarked and documented (existing code)
- [ ] Heuristic selection documented; no behavior change
- [ ] Zero-copy paths proven via perf counters

### 4.3 Vectorized Operations
**Owner:** Query Optimizer  
**Acceptance Criteria:**
- [ ] SIMD aggregate paths measured for ≥1024 batch size
- [ ] Late materialization and parallel scans validated by benches
- [ ] Cost model doc updated; no external API change

## Phase 5: Test Suite Overhaul
**Status:** PENDING  
**Owner:** Test Steward  
**Duration:** 2–3 days  
**Dependencies:** Phase 4 completion  

### 5.1 Test Cleanup
**Owner:** Test Steward  
**Acceptance Criteria:**
- [ ] Remove or update tests pinned to deprecated config or APIs
- [ ] Flaky tests isolated; timeouts and nondeterminism removed
- [ ] Clear categories: unit/integration/perf with labels
- [ ] Suite runtime ≤5 minutes on dev hardware; CI within SLA
- [ ] Coverage ≥80% on critical paths (txn, WAL, compaction, columnar scan)

### 5.2 Integration Test Harness
**Owner:** Test Steward  
**Acceptance Criteria:**
- [ ] Deterministic data generators with configurable sizes
- [ ] Chaos tests (fsync faults, crash injectors) scripted
- [ ] Crash-recovery matrix runs nightly in CI
- [ ] No external services required (embedded DB)

### 5.3 Performance Regression Suite
**Owner:** Test Steward + Query Optimizer  
**Acceptance Criteria:**
- [ ] Automated benches via scripts in `scripts/`
- [ ] Trend tracking JSON committed per run; HTML reports archived
- [ ] Alert/fail threshold at ≥5% regression
- [ ] Flamegraphs/cpu/mem stats captured for hotspots
- [ ] Latency histograms captured and versioned

## Phase 6: Documentation
**Status:** PENDING  
**Owner:** Docs & Runbooks  
**Duration:** 2–3 days  
**Dependencies:** Phase 5 completion  

### 6.1 Architecture Documentation
**Owner:** Docs & Runbooks  
**Acceptance Criteria:**
- [ ] Up-to-date architecture and data flow diagrams
- [ ] Component interaction docs aligned with current code
- [ ] Public API reference consistent with crate exports
- [ ] On-disk format and migration catalog documented

### 6.2 Operational Runbooks
**Owner:** Docs & Runbooks + Risk Officer  
**Acceptance Criteria:**
- [ ] Build, test, bench procedures documented, version-pinned
- [ ] Monitoring & metrics usage documented (no new features)
- [ ] Incident response and rollback playbooks
- [ ] Performance tuning guidance for config knobs

### 6.3 Migration & Backup Guides
**Owner:** Data Quality & Migrations  
**Acceptance Criteria:**
- [ ] Snapshot/backup via existing BackupManager with CLI script
- [ ] Restore + validation procedure with checksum verification
- [ ] PITR documented when WAL available
- [ ] RTO/RPO validated on synthetic datasets; evidence recorded
- [ ] DR drill scripted and documented

## Phase 7: Repository Cleanup
**Status:** PENDING  
**Owner:** PM/Coordinator  
**Duration:** 1–2 days  
**Dependencies:** Phase 6 completion  

### 7.1 File Cleanup
**Owner:** PM/Coordinator  
**Acceptance Criteria:**
- [ ] Remove/trim stale reports; move legacy artifacts to `/archive`
- [ ] Drop unused examples or align with APIs
- [ ] Remove build/test artifacts; ensure `.gitignore` covers them
- [ ] Keep repo navigable with a clear top-level README

### 7.2 Configuration Consolidation
**Owner:** PM/Coordinator  
**Acceptance Criteria:**
- [ ] Single source of truth for config docs
- [ ] Feature flags enumerated and described
- [ ] Build profiles aligned; strip unused profiles
- [ ] Toolchain pinned via `rust-toolchain.toml`

## Phase 8: Final Validation
**Status:** PENDING  
**Owner:** Risk Officer + PM/Coordinator  
**Duration:** 1–2 days  
**Dependencies:** All phases complete  

### Acceptance Criteria
- [ ] All phases completed with evidence
- [ ] Performance metrics meet/exceed baseline
- [ ] Security scan clean
- [ ] Documentation complete and accurate
- [ ] Rollback procedures tested
- [ ] Sign-off from all sub-agents

---

## Ownership Matrix (Sub-Agents)
- Schema Auditor: Phase 3.3, 6.1 (format/catalog)
- Query Optimizer: Phases 3.1–3.2, 4.3, 5.3
- OLAP Architect: Phase 4.1–4.2
- Data Quality & Migrations: Phase 6.3
- Test Steward: Phases 2.1, 5.1–5.3
- Docs & Runbooks: Phase 6.1–6.2
- Risk Officer: Phases 2.3, 6.2, 8
- PM/Coordinator: Phases 2.2, 7, overall cadence

## Risk & Mitigation Updates
- Build breaks: gate merges on CI + bench smoke tests
- Perf regressions: 5% threshold auto-fail with rollback to baseline
- Data integrity: mandatory backup/restore drill before risky changes

## Timeline & ETAs (Ranges)
- Phase 2: 2–4 days
- Phase 3: 3–4 days
- Phase 4: 3–4 days
- Phase 5: 2–3 days
- Phase 6: 2–3 days
- Phase 7: 1–2 days
- Phase 8: 1–2 days

## Evidence & Artifacts
- Bench reports: `benchmark_results/<date>/`
- Security scans: `security_reports/<date>/`
- ADRs: `docs/adr/NNN-title.md`
- Baselines: `performance-baselines.json`

## Risk Register

### High Risk Items
1. **Compilation Errors**: Multiple compilation failures blocking progress
   - **Mitigation**: Fix incrementally, test each fix
2. **Performance Regression**: Changes may degrade performance
   - **Mitigation**: Benchmark before/after each change
3. **Data Corruption**: Schema changes could corrupt data
   - **Mitigation**: Always backup before migrations

### Medium Risk Items
1. **Test Flakiness**: Intermittent test failures
   - **Mitigation**: Isolate and fix or remove flaky tests
2. **Documentation Drift**: Docs becoming outdated
   - **Mitigation**: Update docs with each code change

## Success Metrics

### OLTP Targets
- p50 latency: <1ms
- p95 latency: <10ms  
- p99 latency: <50ms
- TPS: >100K for simple queries
- Deadlock rate: <0.01%

### OLAP Targets
- Full scan: <100ms per GB
- Aggregation: >1M rows/sec
- Compression: >5x
- Join performance: >100K rows/sec
- Memory usage: <2x data size

### Operational Targets
- Startup time: <1 second
- Shutdown time: <5 seconds
- Backup speed: >100MB/sec
- Recovery time: <5 minutes
- Zero data loss guaranteed

## Timeline
- **Total Duration:** 22 working days
- **Critical Path:** Phase 2 → Phase 3 → Phase 4
- **Parallel Work:** Documentation can start after Phase 3
- **Buffer:** 3 days for unexpected issues

## Next Steps
1. Begin Phase 2.1: Fix compilation errors
2. Set up continuous benchmarking
3. Create backup of current state
4. Initialize change tracking log
