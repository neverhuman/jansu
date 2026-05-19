Agent

- Codex

Prompt

- Implement the previous agent's Jankurai score regression recovery plan in a fresh context, preserving existing dirty work and recovering only targeted, reviewed pieces.

Phase Or Audit Item

- `cross-phase` / `AUDIT-019`: Jankurai score recovery
- Related: `AUDIT-001`, `AUDIT-018`, and `AUDIT-004` score-proof residual

Files Read

- `/home/ubuntu/.codex/RTK.md`
- `AGENTS.md`
- `MASTER_PLAN.md`
- `AUDIT.md`
- `phase-logs/README.md`
- `phase-logs/index.json`
- `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `phase-logs/attempts/cross-phase/20260518-133636-codex-jankurai.md`
- `agent/repo-score.json`
- `agent/repo-score.md`
- `.github/workflows/ci.yml`
- `.github/workflows/differential-kafka-lab.yml`
- `.github/workflows/jankurai.yml`
- `.github/workflows/release.yml`
- `justfile`
- `scripts/ci-local.sh`
- `tools/checks/jankurai-gate.sh`
- Selected reference files from `origin/ws-ij-ci-tool-adoption-fast-lanes`
- Selected reference files from `origin/ws-h-ownership-security-docs`
- Selected reference files from `origin/ws-c-sql-destructive-delete-proof`
- Selected reference files from `jankurai-score`

Files Changed

- `.github/workflows/ci.yml`
- `.github/workflows/differential-kafka-lab.yml`
- `.github/workflows/jankurai.yml`
- `.github/workflows/release.yml`
- `.gitleaks.toml`
- `AUDIT.md`
- `agent/JANKURAI_STANDARD.md`
- `agent/audit-policy.toml`
- `agent/boundaries.toml`
- `agent/generated-zones.toml`
- `agent/owner-map.json`
- `agent/proof-lanes.toml`
- `agent/security-policy.toml`
- `agent/standard-version.toml`
- `agent/test-map.json`
- `agent/tool-adoption.toml`
- `agent/ux-qa.toml`
- `contracts/`
- `db/`
- `docs/architecture/README.md`
- `docs/db/destructive-delete-proof.md`
- `docs/decisions/README.md`
- `docs/exceptions/README.md`
- `docs/install.md`
- `docs/ops/cost-budget.md`
- `docs/release.md`
- `docs/security/agent-tool-supply.md`
- `docs/security/authz-matrix.md`
- `docs/security/input-boundary.md`
- `docs/testing.md`
- `justfile`
- `ops/`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `phase-logs/attempts/cross-phase/20260518-141908-codex-jankurai-recovery.md`
- `phase-logs/index.json`
- `scripts/ci-doctor.sh`
- `scripts/ci-local.sh`
- `tools/checks/jankurai-gate.sh`
- `tools/jankurai-rust/witness.sh`
- `tools/security-lane.sh`

Tests Added

- None planned for docs/tooling metadata.

Verification Commands

- `rtk bash -n scripts/ci-local.sh scripts/ci-doctor.sh tools/checks/jankurai-gate.sh tools/security-lane.sh ops/ci/*.sh tools/jankurai-rust/witness.sh` - passed.
- `rtk actionlint .github/workflows/*.yml` - passed.
- `rtk just --list` - passed.
- `rtk git diff --check` - passed.
- `rtk jq empty phase-logs/index.json` - passed.
- `rtk cargo fmt --all --check` - passed.
- `rtk just tool-adoption-evidence` - passed.
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-jankurai-recovery bash scripts/ci-local.sh audit` - failed as designed because the strict zero-finding gate is not clean: `score=64`, `caps=14`, `findings=167`.

Outcome

- Added non-behavioral Jankurai policy/proof metadata under `agent/`.
- Added concise owner/security/release/testing/cost/input-boundary/destructive-delete documentation.
- Added `ops/ci` helper scripts, a CI doctor, pre-push hook, and security lane script.
- Pinned workflow actions, added timeouts/concurrency where missing, added security evidence and SARIF upload surfaces, and kept the dedicated Jankurai workflow thin through `ops/ci/jankurai.sh`.
- The current installed auditor is Jankurai `1.5.0`; current generated score is `64` with `14` caps and `167` findings.

Residual Risks

- The strict gate still fails; remaining caps are `no-jankurai-audit-lane-in-ci`, product-code placeholder/fallback/dead-language caps, authz/input/release/docs/streaming-runtime caps, Rust/SQL/Docker bad-behavior caps, and comment hygiene.
- The direct-audit workflow heuristic conflicts with the thin-workflow parity heuristic in Jankurai `1.5.0`; this pass kept the workflow thin per repository contract and left the audit-lane cap as a residual.
- Product-code, SQL, Docker, authz, input-boundary, and release-readiness findings were not fixed in this cross-phase metadata pass.

Next Recommended Action

- Continue with phase-owned remediation: schema/sans-io dead-marker cleanup, storage SQL destructive-delete proof or query rewrites, Docker compose binding fixes, and Phase 14 authz/input-boundary proof. Revisit the Jankurai audit-lane heuristic with a dedicated accepted baseline or upstream-compatible workflow pattern before making the workflow required.
