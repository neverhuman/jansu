# Agent

Codex GPT-5

## Prompt

Implement the provided strict-gate-first then RedlineDB migration plan in a fresh context.

## Phase Or Audit Item

`AUDIT-020` cross-phase RedlineDB storage migration authority, with a matching Phase 17 scaffold.

## Files Read

- `AGENTS.md`
- `MASTER_PLAN.md`
- `AUDIT.md`
- `phase-logs/index.json`
- `phase-logs/README.md`
- `tips/phases/16-ecosystem-performance-ops-migration.md`
- `tips/phases/17-redlinedb-storage-migration.md`
- `phase-logs/17-redlinedb-storage-migration.md.log`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `phase-logs/attempts/cross-phase/20260520-094642-codex-pr13-verification.md`
- `.github/workflows/ci.yml`
- `.github/workflows/jankurai.yml`
- `jansu-storage/src/storage_box.rs`
- `jansu-storage/src/service/list_offsets.rs`
- `jansu-storage/src/limbo/offsets.rs`
- `jansu-storage/src/lite/storage_topics.rs`

## Files Changed

- `MASTER_PLAN.md`
- `AUDIT.md`
- `tips/phases/17-redlinedb-storage-migration.md`
- `phase-logs/17-redlinedb-storage-migration.md.log`
- `phase-logs/index.json`

## Tests Added

- None.

## Verification Commands

- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-redlinedb-governance cargo test -p jansu-broker --test compatibility_contract --all-features -- phase_log_manifest_contract_covers_phase_docs --nocapture`
- `rtk cargo fmt --all --check`
- `rtk git diff --check`
- `rtk actionlint .github/workflows/*.yml`
- `rtk bash tools/security-lane.sh`
- `rtk rm -f target/jankurai/audit-state.json && rtk just score`

## Outcome

- Created the missing repository-backed RedlineDB migration authority and matching phase scaffold.
- Confirmed the manifest contract still passes, formatting and diff hygiene are clean, workflow lint and the security lane pass, and the full Jankurai gate still reproduces the known `HLT-001` and `HLT-018` residuals.

## Residual Risks

- Strict-gate residuals `HLT-001` and `HLT-018` remain unresolved.
- The RedlineDB migration itself still needs helper, harness, and workflow implementation.

## Next Recommended Action

- Implement the Phase 17 helper/harness work and then retire the legacy SQL-engine defaults.
