Agent: Codex

Prompt: Run one more 10-worker Jankurai triage pass, improve worker usefulness, and carry safe audit fixes forward quickly.

Phase Or Audit Item: AUDIT-019 cross-phase Jankurai score recovery.

Files Read:
- AGENTS.md
- AUDIT.md
- phase-logs/index.json
- agent/repo-score.json
- agent/repo-score.md
- jansu-cli/src/cli/perf.rs
- jansu-auth/src/lib.rs
- jansu-broker/src/coordinator/group/administrator/forming.rs
- jansu-broker/src/service/safe_errors.rs
- jansu-cat/src/consume.rs
- jansu-storage/src/sql/consumer_offset_delete_by_topic.sql
- jansu-storage/src/sql/scram_credential_delete.sql
- jansu-storage/src/sql/topic_configuration_delete.sql
- jansu-storage/src/sql/*.sql
- jansu-storage/src/lite/*.sql

Files Changed:
- AUDIT.md
- phase-logs/index.json
- phase-logs/attempts/cross-phase/20260518-190325-codex-jankurai-worker-followup.md
- jansu-cli/src/cli/perf.rs
- jansu-auth/src/lib.rs
- jansu-broker/src/coordinator/group/administrator/forming.rs
- jansu-broker/src/service/safe_errors.rs
- jansu-cat/src/consume.rs
- jansu-storage/src/sql/consumer_offset_delete_by_topic.sql
- jansu-storage/src/sql/scram_credential_delete.sql
- jansu-storage/src/sql/topic_configuration_delete.sql
- jansu-storage/src/sql/*.sql
- jansu-storage/src/lite/*.sql

Tests Added: None.

Verification Commands:
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-jankurai-cli cargo check -p jansu-cli --no-default-features --features dynostore`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-jankurai-auth cargo check -p jansu-auth --all-targets --all-features`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-jankurai-broker cargo check -p jansu-broker --no-default-features`
- `rtk env CARGO_TARGET_DIR=/tmp/jansu-verify-jankurai-cat cargo check -p jansu-cat --no-default-features`
- `rtk cargo fmt --all --check`
- `rtk git diff --check`
- `rtk jankurai doctor --fail-on critical`
- `rtk just score`

Outcome:
- Ran another 10-worker read-only Jnoccio pass. Worker output was still not reliably machine-structured, but the summaries were actionable enough to identify low-risk HLT-030 SQL formatting, a CLI `todo!()` branch, and fallback-soup idioms.
- Replaced the `jansu-cli` perf consume `todo!()` with an explicit unsupported error.
- Replaced targeted fallback chains in auth, broker safe-error shaping, broker offset-fetch topic expansion, and `jansu-cat` consume JSON/response handling with explicit matches.
- Reformatted destructive delete SQL assets so Jankurai no longer reports HLT-030 findings.
- Moved the strict gate from `score=64 raw=72 caps=9 findings=135` to `score=64 raw=72 caps=8 findings=109`.

Residual Risks:
- `rtk just score` still fails because strict Jankurai requires zero caps and zero findings.
- HLT-001 remains the dominant cluster: shape finding for `jansu-storage/src/lite.rs`, 97 vibe findings across schema/sans-io/model and fallback idioms, and caps for vibe placeholders, fallback soup, and future-hostile wording.
- Fallback-soup findings advance to the next matching site after each localized cleanup; the current artifact exposes `jansu-cat/src/produce.rs:279`, and a repo-wide scan still finds many `.ok()` / `unwrap_or_default()` idioms. Clearing that cap needs a coordinated idiom cleanup, not one-off patches.
- Remaining singleton proof/config findings cover authz/data isolation, input-boundary, release readiness, exception/observability proof, streaming runtime drift, and human-review proof.

Next Recommended Action:
- Treat HLT-001 as two separate batches: first replace real `todo!()` / `unimplemented!()` runtime branches in schema/sans-io with typed errors, then run a repo-wide fallback idiom cleanup with focused package checks.
- Leave protocol semantic terms such as `deprecated`, `old`, and `stale` alone unless an owning phase proves a safe rename, because workers identified several as wire/protocol vocabulary rather than dead code markers.
