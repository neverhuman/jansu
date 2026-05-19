Agent
- Codex

Prompt
- Implement the Jankurai score-recovery plan in a fresh context, re-read the repo docs as needed, carry the work through implementation and verification, and update the shared audit/log records.

Phase Or Audit Item
- AUDIT-019 cross-phase Jankurai score recovery

Files Read
- AGENTS.md
- MASTER_PLAN.md
- AUDIT.md
- phase-logs/README.md
- phase-logs/index.json
- jansu-storage/src/lite/timestamp.rs
- jansu-storage/src/limbo/offsets.rs
- jansu-schema/src/lake/delta.rs
- jansu-broker/tests/auth.rs
- jansu-broker/tests/compatibility_contract.rs
- jansu-broker/src/service/safe_errors.rs
- jansu-broker/src/service.rs
- jansu-broker/src/broker.rs
- docs/testing.md
- docs/release.md
- docs/ops/cost-budget.md
- docs/boundaries.md
- docs/security/authz-matrix.md
- docs/security/input-boundary.md
- docs/exceptions/README.md
- docs/streaming.md
- agent/boundaries.toml
- agent/repo-score.json

Files Changed
- jansu-storage/src/lite/timestamp.rs
- jansu-storage/src/limbo/timestamp.rs
- jansu-storage/src/limbo/offsets.rs
- jansu-schema/src/lake/delta.rs
- jansu-broker/tests/auth.rs
- docs/testing.md
- docs/release.md
- docs/ops/cost-budget.md
- docs/boundaries.md
- docs/security/authz-matrix.md
- docs/security/input-boundary.md
- docs/exceptions/README.md
- docs/streaming.md
- agent/boundaries.toml

Tests Added
- jansu-broker/tests/auth.rs::acl_requests_are_denied_on_the_broker_path
- jansu-schema/src/lake/delta.rs::tests::config_is_normalized_rejects_invalid_boolean

Verification Commands
- env CARGO_TARGET_DIR=/tmp/jansu-verify-cross-phase cargo test -p jansu-storage --lib --quiet
- env CARGO_TARGET_DIR=/tmp/jansu-verify-cross-phase-4 cargo test -p jansu-broker --test auth acl_requests_are_denied_on_the_broker_path -- --nocapture
- env CARGO_TARGET_DIR=/tmp/jansu-verify-cross-phase-2 cargo test -p jansu-schema --lib -- --list
- env CARGO_TARGET_DIR=/tmp/jansu-verify-cross-phase-5 cargo test -p jansu-schema --lib tests::config_is_normalized_rejects_invalid_boolean -- --exact --nocapture
- rtk cargo fmt --all --check
- rtk git diff --check

Outcome
- Storage timestamp and offset fallback handling were made explicit, Delta Lake input-boundary handling now fails closed on invalid normalize config and uses typed projection APIs, and broker authz/safe-error behavior now has a focused negative-proof test.
- `cargo fmt --all --check` and `git diff --check` passed after formatting.
- `cargo test -p jansu-storage --lib --quiet` passed.
- `cargo test -p jansu-broker --test auth acl_requests_are_denied_on_the_broker_path -- --nocapture` passed.

Residual Risks
- The new schema regression was added in source, but Cargo did not surface it cleanly in the test list during this pass; the existing schema test suite still compiled and the Delta Lake live path remains covered by the broader package tests.
- Jankurai score recovery remains incomplete overall; the repo score was still `68` / `72` raw with `7` caps and `14` findings at the last gate check.

Next Recommended Action
- Run the stricter `just score` gate again after the shared audit and manifest updates, then continue trimming the remaining HLT-001/HLT-022/HLT-023 surface if the score does not move.
