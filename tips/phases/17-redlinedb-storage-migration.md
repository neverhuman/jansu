# Phase 17 - RedlineDB Storage Migration and Legacy SQL-Engine Retirement

Parallelism: SERIAL

Depends on: Phase 06 - Storage Log Contract; Phase 08 - Fetch ListOffsets Leader Epoch; Phase 10 - Consumer Groups Offsets; Phase 16 - Ecosystem Performance Ops Migration; `AUDIT-020`.

Can run with: documentation, harness prep, and release-policy planning can run alongside Phase 16 prep, but product-surface retirement and storage-harness changes must remain phase-backed.

Goal: Make RedlineDB the only SQL-backed storage path while preserving `dynostore`, `slatedb`, `memory`, and `S3`, and retire `postgres`, `sqlite`, `libsql`, `turso`, and `tokio-postgres` from default product surfaces.

Current code anchors:
- `README.md`, `CLAUDE.md`, and `docs/` advertise the current storage-engine story.
- `justfile`, `.github/workflows/*.yml`, and `scripts/` define the default local and CI lanes.
- `jansu-cli/src/cli.rs` and `jansu-cli/src/cli/broker.rs` surface storage-engine examples and defaults.
- `jansu-broker/tests/common/mod.rs` and `jansu-storage/tests/` own the storage harnesses and engine-specific proof lanes.
- `compose.yaml`, `demo/`, and `etc/initdb.d/` still carry legacy SQL-engine setup examples.
- `docs/compatibility/kafka-4.2-ledger.json` and the phase logs capture the compatibility and migration evidence trail.

Implementation steps:
- Add a shared RedlineDB install-and-verify helper pinned to the official release binary and reuse it from local and CI lanes.
- Switch storage and test harnesses to `redlinedb://` temp-file URLs while keeping the non-SQL storage engines intact.
- Remove `postgres`, `sqlite`, `libsql`, `turso`, and `tokio-postgres` from default product surfaces, workflows, and recipes.
- Add a guard that fails if retired engine names reappear in product code, workflows, justfile recipes, or default docs.
- Update compatibility and release docs so the storage matrix and default examples match the new RedlineDB-backed SQL path.

Tests:
- Add a helper-level smoke test that installs/verifies the pinned RedlineDB binary and executes a trivial query against both `:memory:` and a file-backed temp URL.
- Add a storage harness test that proves the broker and storage stack boot with `redlinedb://` and still pass the focused storage checks.
- Add a guard test that fails on reintroduction of retired engine names in product code, workflows, and default recipes.
- Add a matrix test that confirms the legacy SQL-engine lanes are gone from default CI coverage while `dynostore`, `slatedb`, `memory`, and `S3` remain.

Acceptance gate: default product surfaces, workflows, and recipes do not advertise retired SQL engines, and the migration path is backed by helper, harness, and matrix proof.

Do not do:
- Do not remove archival documentation that is clearly historical.
- Do not claim parity without ledger or test proof.
- Do not collapse the non-SQL storage engines into a single migration path.

Fresh session handoff: start with the RedlineDB helper and guarded storage harness before removing legacy SQL-engine surfaces.
