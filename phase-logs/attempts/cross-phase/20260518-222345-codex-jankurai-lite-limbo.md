# Agent

Codex

# Prompt

Implement the prior 10-worker Jankurai audit recovery plan in a fresh context, preserving the repo contract and using phase-backed/audit-backed work.

# Phase Or Audit Item

`AUDIT-019` cross-phase Jankurai score recovery, with storage-shape cleanup touching Phase 08/10/13-owned storage backend files.

# Files Read

- `AGENTS.md`
- `/home/ubuntu/.codex/RTK.md`
- `MASTER_PLAN.md`
- `AUDIT.md`
- `phase-logs/index.json`
- `phase-logs/README.md`
- `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `phase-logs/10-consumer-groups-offsets.md.log`
- `agent/repo-score.md`
- `agent/repo-score.json`
- `jansu-storage/src/lite.rs`
- `jansu-storage/src/limbo.rs`
- `jansu-storage/src/lib.rs`
- `jansu-storage/Cargo.toml`

# Files Changed

- `jansu-storage/src/lite.rs`
- `jansu-storage/src/lite/mod.rs`
- `jansu-storage/src/lite/builder.rs`
- `jansu-storage/src/lite/connection.rs`
- `jansu-storage/src/lite/engine.rs`
- `jansu-storage/src/lite/storage.rs`
- `jansu-storage/src/lite/tests.rs`
- `jansu-storage/src/lite/timestamp.rs`
- `jansu-storage/src/limbo.rs`
- `AUDIT.md`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `phase-logs/index.json`
- `phase-logs/attempts/cross-phase/20260518-222345-codex-jankurai-lite-limbo.md`

# Tests Added

- None.

# Verification Commands

- `rtk rustfmt --edition 2024 jansu-storage/src/lite/mod.rs jansu-storage/src/lite/builder.rs jansu-storage/src/lite/connection.rs jansu-storage/src/lite/engine.rs jansu-storage/src/lite/storage.rs jansu-storage/src/lite/tests.rs jansu-storage/src/lite/timestamp.rs jansu-storage/src/limbo.rs`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-worker-a-lite cargo check -p jansu-storage --no-default-features --features libsql --all-targets`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-worker-b-limbo cargo check -p jansu-storage --no-default-features --features turso`
- `rtk git diff --check`
- `rtk cargo fmt --all --check` (failed on pre-existing unrelated formatting drift outside the Lite/Limbo files touched here)
- `rtk just score`

# Outcome

- Split the libSQL Lite backend from one 5,883-line `lite.rs` into `lite/mod.rs` plus focused internal modules while preserving the `mod lite;` public module entrypoint.
- Replaced Limbo transaction/add-partitions/SCRAM dead-marker fallbacks with explicit protocol errors or disabled-security responses.
- Confirmed the harmful Limbo structural split increased duplication pressure, then backed out that split while keeping the explicit fallback fixes.
- Jankurai now reports `score=68`, `raw=72`, `caps=7`, and `findings=14`; strict gate still fails because caps/findings are nonzero.

# Residual Risks

- HLT-001 shape pressure remains, now led by `jansu-storage/src/limbo.rs`.
- Remaining caps are `vibe-placeholders-in-product-code`, `fallback-soup-in-product-code`, `authz-or-data-isolation-gap`, `input-boundary-gap`, `release-readiness-gap`, `no-agent-friendly-exception-pattern`, and `streaming-runtime-drift`.
- `cargo check -p jansu-storage --no-default-features --features turso --all-targets` remains blocked by pre-existing Limbo unit tests that reference optional `libsql` while only `turso` is enabled; the production Turso backend check passes.
- Workspace-wide `cargo fmt --all --check` still fails on unrelated dirty files already outside this attempt's write set; the touched Lite/Limbo Rust files were formatted directly with `rustfmt --edition 2024`.

# Next Recommended Action

Keep the Lite split. Do not reattempt a naive Limbo module split; it triggers the severe duplication cap. The next useful HLT-001 pass should either split `jansu-storage/src/lib.rs`/shared storage dispatch deliberately or refactor Limbo behavior into real helper APIs with tests instead of moving one large trait impl verbatim.
