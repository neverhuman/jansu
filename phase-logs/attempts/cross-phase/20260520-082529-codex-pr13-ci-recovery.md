## Agent

Codex GPT-5

## Prompt

Implement the previous agent's PR 13 Failure Recovery Plan in a fresh context, then configure the machine-wide Cargo default to use 20-30 build workers.

## Phase Or Audit Item

`AUDIT-019` cross-phase Jankurai/CI recovery, with a Phase 08 manifest-contract repair.

## Files Read

- `AGENTS.md`
- `MASTER_PLAN.md`
- `AUDIT.md`
- `phase-logs/index.json`
- `phase-logs/README.md`
- `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- `.github/workflows/ci.yml`
- `.github/workflows/jankurai.yml`
- `ops/ci/lib.sh`
- `ops/ci/jankurai.sh`
- `ops/ci/jankurai-audit.sh`
- `scripts/ci-local.sh`
- `tools/checks/jankurai-gate.sh`
- `tools/security-lane.sh`
- Storage, service, broker, cat, and schema files touched by clippy/Jankurai findings.

## Files Changed

- `.github/workflows/ci.yml`
- `.github/workflows/jankurai.yml`
- `jansu-broker/src/service/safe_errors.rs`
- `jansu-cat/src/consume.rs`
- `jansu-schema/src/json/arrow/tests.rs`
- `jansu-schema/src/proto/config.rs`
- `jansu-service/src/frame.rs`
- `jansu-storage/src/dynostore/dispatch/produce.rs`
- `jansu-storage/src/dynostore/dispatch/topics.rs`
- `jansu-storage/src/dynostore/opticon.rs`
- `jansu-storage/src/groups.rs`
- `jansu-storage/src/limbo/offsets.rs`
- `jansu-storage/src/limbo/timestamp.rs`
- `jansu-storage/src/limbo/topics.rs`
- `jansu-storage/src/limbo/topics_config.rs`
- `jansu-storage/src/limbo/transactions.rs`
- `jansu-storage/src/limbo/txn_produce.rs`
- `jansu-storage/src/lite/storage_config.rs`
- `jansu-storage/src/lite/storage_topics.rs`
- `jansu-storage/src/lite/storage_txn.rs`
- `jansu-storage/src/lite/timestamp.rs`
- `jansu-storage/src/pg/dispatch/configs.rs`
- `jansu-storage/src/pg/dispatch/txn.rs`
- `jansu-storage/src/service/create_topics.rs`
- `jansu-storage/src/service/delete_topics.rs`
- `jansu-storage/src/service/describe_configs.rs`
- `jansu-storage/src/service/incremental_alter_configs.rs`
- `jansu-storage/src/service/list_offsets.rs`
- `jansu-storage/src/service/txn/offset_commit.rs`
- `jansu-storage/src/storage_box.rs`
- `phase-logs/index.json`
- `AUDIT.md`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `phase-logs/attempts/cross-phase/20260520-082529-codex-pr13-ci-recovery.md`
- `/home/ubuntu/.cargo/config.toml`

## Tests Added

- None.

## Verification Commands

- `rtk python3 /home/ubuntu/.codex/plugins/cache/openai-curated/github/ed8ce2ea/skills/gh-fix-ci/scripts/inspect_pr_checks.py --repo . --pr 13 --max-lines 120 --context 30` (reported current PR failures)
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-pr13-repro cargo clippy --workspace --all-features --all-targets -- -D warnings` (failed before fixes, passed after fixes)
- `rtk bash tools/checks/jankurai-gate.sh` (failed before fixes with caps; after fixes reports `score=83 raw=83 caps=0 findings=2`)
- `rtk bash tools/security-lane.sh` (passed)
- `rtk actionlint .github/workflows/*.yml` (passed)
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-pr13-fix cargo test -p jansu-broker --test compatibility_contract --all-features -- phase_log_manifest_contract_covers_phase_docs --nocapture` (passed)
- `rtk just fmt` (passed)
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-pr13-repro just check` (passed)
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-pr13-repro just clippy` (passed)
- `rtk just security` (passed)
- `rtk rm -f target/jankurai/audit-state.json && rtk just jankurai-gate` (failed only on the two known Jankurai residual findings: `HLT-001` shape and `HLT-018` build speed)
- `rtk env CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=24 CARGO_TARGET_DIR=/tmp/jansu-verify-pr13-matrix bash -lc '...'` for the CI storage/lake matrix (partially passed; failed during a later `jansu` binary link with `rust-lld` signal 7 after the filesystem dropped below 500 MiB free)
- `rtk git diff --check` (passed)

## Outcome

- Installed `just` in the Jankurai workflow so the remote audit job can run `ops/ci/jankurai.sh`.
- Split CI security tool installation into explicit `cargo-audit`, `actionlint`, and `gitleaks` steps; `cargo-audit` install is advisory, matching the lane's advisory audit semantics, while `actionlint`, `gitleaks`, and `cargo` remain required by `tools/security-lane.sh`.
- Cleared the workspace clippy regressions under `-D warnings` with mechanical defaulting and duplicated-branch cleanups.
- Fixed the Phase 08 manifest contract by replacing stale `jansu-storage/src/slate/types.rs` with `jansu-storage/src/slate/types/mod.rs`.
- Removed new Jankurai hard caps introduced by the clippy cleanup path. The strict full scan now has zero caps and two known findings.
- Set global Cargo build parallelism to 24 workers in `/home/ubuntu/.cargo/config.toml`.

## Residual Risks

- `just jankurai-gate` still fails on two known `AUDIT-019` residual findings: `HLT-001` shape (`jansu-sans-io/src/lib.rs` and other large files) and `HLT-018` build-speed scoring.
- The full local storage/lake matrix did not finish because the machine filesystem filled during linking; `/tmp/jansu-verify-pr13-matrix` was removed afterward to restore space.
- GitHub Actions was inspected but not re-run from this session.

## Next Recommended Action

Push this branch and re-run PR 13 checks. If the matrix still fails remotely, inspect the exact failing storage/lake job logs; if Jankurai now runs to the strict gate, decide whether to keep the current zero-baseline policy and finish the remaining `AUDIT-019` shape/build-speed work, or document a true false-positive baseline.
