# ADR 0002: Baselines and Governance Setup

Date: 2025-09-01

Status: Accepted

Context
- Establish deterministic toolchain, ADR template, and artifact directories.
- Guardrails: no new features; reproducible scripts; safety-first with snapshots.

Decision
- Pin Rust toolchain via `rust-toolchain.toml` to 1.79.0 with `rustfmt` and `clippy` components.
- Add ADR template `docs/adr/0000-template.md` for all subsequent changes.
- Standardize artifacts directories:
  - `benchmark_results/<date>/`
  - `security_reports/<date>/`
  - `snapshots/<date>/`

Consequences
- Improves reproducibility and reviewability.
- Slight maintenance overhead to update toolchain/version when needed (requires ADR).

Evidence
- `rust-toolchain.toml` present and used by local builds.
- Repo builds with the pinned toolchain.

Rollout & Rollback
- Rollout: Commit files; ensure CI uses pinned toolchain.
- Rollback: Remove `rust-toolchain.toml` and template if necessary; low risk.

