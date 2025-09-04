# ADR 0003: Toolchain Alignment to Pinned Nightly

Date: 2025-09-01
Status: Accepted

Context
- The repository pins the Rust toolchain via `rust-toolchain.toml` to `nightly-2025-02-01` with `rustfmt` and `clippy` components.
- ADR 0002 previously referenced a stable toolchain; this creates drift in governance vs. code.

Decision
- Align governance to the current pinned nightly version `nightly-2025-02-01` to ensure reproducible builds matching the repository configuration.
- CI and local developers MUST use the pinned toolchain (`rustup override` honored by rust-toolchain file).
- Any future change to the toolchain requires an ADR documenting motivation, risk, and rollback, and MUST include: `cargo check`, `cargo test --no-run`, quick perf smoke (OLTP quick baseline).

Consequences
- Nightly provides access to features/optimizations potentially used by dependencies; ensures consistency across contributors.
- Potential for nightly breakage is mitigated by pinning to an exact date; updates require explicit ADRs.

Evidence
- `rust-toolchain.toml` exists with nightly pin; local builds pick it up.
- Successful dry-run compilation and benches under the pinned nightly will be recorded in baseline artifacts.

Rollout
- Ensure CI uses the pinned nightly (no matrix drift). Document in contributor guide.

Rollback
- If nightly exhibits issues, revert to last known good nightly or adopt stable via a follow-up ADR, together with minimal compatibility fixes and re-baselining.

