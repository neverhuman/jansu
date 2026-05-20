## Agent

Codex

## Prompt

Implement the previously produced plan for PR CI parity and patch release bump, then verify the branch against the repo's CI-relevant lanes.

## Phase Or Audit Item

Cross-phase release/version bump and CI parity maintenance

## Files Read

- `AGENTS.md`
- `/home/ubuntu/.codex/RTK.md`
- `MASTER_PLAN.md`
- `AUDIT.md`
- `phase-logs/index.json`
- `Cargo.toml`
- `Cargo.lock`
- `docs/release.md`
- `agent/boundaries.toml`
- `justfile`
- `ops/ci/lib.sh`
- `ops/ci/release.sh`
- `ops/ci/test.sh`
- `.github/workflows/ci.yml`
- `.github/workflows/jankurai.yml`
- `scripts/ci-local.sh`
- `tips/phases/17-redlinedb-storage-migration.md`

## Files Changed

- `Cargo.toml`
- `Cargo.lock`
- `docs/release.md`
- `agent/boundaries.toml`
- `justfile`
- `AUDIT.md`

## Tests Added

- None.

## Verification Commands

- `rtk git diff --check`
- `rtk just fmt`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-release-bump just check`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-clippy just clippy`
- `rtk actionlint .github/workflows/*.yml`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-release just release-check`  
- `rtk just release-check`
- `rtk just test`
- `rtk env SCHEMA_REGISTRY=file://./etc/schema just test`

## Outcome

- Workspace version bumped from `0.6.2` to `0.6.3` in the root manifest and workspace dependency pins.
- Release-facing version/tag guidance in `docs/release.md` now matches the new patch release.
- `agent/boundaries.toml` now carries the updated stack version.
- `just build-storage` now includes `dynostore` so the local helper better matches the CI storage matrix.
- `cargo check`, `clippy`, `actionlint`, and `release-check` passed.
- `just test` remains a weak local signal here: the unqualified run failed on missing schema registry/db environment, and the CI-shaped rerun still surfaced existing environment-dependent failures in Postgres-backed and some storage tests.

## Residual Risks

- The full workspace test lane still needs a proper CI-equivalent environment to distinguish setup gaps from real product regressions.
- Local `just test` is not yet fully CI-parity because it does not inject the workflow's runtime env automatically.

## Next Recommended Action

Push the branch and let GitHub Actions validate the remaining workspace test surface in the real CI environment.
