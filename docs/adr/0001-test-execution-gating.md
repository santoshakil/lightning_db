# ADR 0001: Gate Heavy Integration/Example Targets Behind Feature Flag

Status: Accepted
Date: 2025-09-01

Context
- The repository contains numerous long-running integration/stress tests and example/benchmark binaries.
- Many of these targets reference deprecated config fields or unstable APIs and are not required to validate core library correctness.
- To stabilize and harden the codebase without changing product behavior, we need fast, deterministic test runs and a clean baseline.

Decision
- Introduce a Cargo feature `integration_tests` and gate heavy test modules and examples behind it.
- Default `cargo test` runs unit and lightweight integration tests only.
- Heavy suites run explicitly via: `cargo test --features integration_tests`.
- Examples used for performance and operational validation also require `integration_tests`.

Consequences
- Speeds up default CI/test runs and reduces flakiness while we harden the system.
- Avoids compiling stale example/benchmark code paths during unit test runs.
- No product behavior changes; only test/build configuration.

Alternatives Considered
- Deleting heavy tests: rejected; we want to keep them runnable.
- Moving tests to a separate workspace crate: deferred; feature gating is lower friction.

Verification
- Confirm unit tests pass by default.
- Run heavy suites explicitly and triage failures incrementally.

