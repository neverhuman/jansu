# Phase 08 Attempt 001 — Leader Epoch Truthfulness & API 23 Routing

- **Agent**: Antigravity/Gemini
- **Prompt**: "progress on MASTER_PLAN and audit"
- **Phase Or Audit Item**: Phase 08, AUDIT-004
- **Date**: 2026-05-03

## Files Read

- `jansu-storage/src/service/list_offsets.rs` — identified hardcoded `.leader_epoch(Some(0))`
- `jansu-storage/src/service/fetch.rs` — confirmed epoch fencing logic
- `jansu-storage/src/service/offset_for_leader_epoch.rs` — confirmed service exists
- `jansu-storage/src/lib.rs` — `ListOffsetResponse`, `OffsetStage`, `StorageContainer` enum, `Storage` trait
- `jansu-storage/src/dynostore.rs` — `Meta` struct, `offset_for_leader_epoch` stub
- `jansu-storage/src/lite.rs` — SQL-backed epoch, found pre-existing `sql_lookup` bug
- `jansu-storage/src/limbo.rs` — SQL-backed epoch via `sql_lookup`
- `jansu-storage/src/slate/storage.rs` — SlateDB stub
- `jansu-storage/src/sql/offset_for_leader_epoch.sql`
- `jansu-storage/src/sql/leader_epoch_history.sql`
- `jansu-storage/tests/offset_for_leader_epoch.rs`
- `jansu-broker/src/service/storage.rs` — route stack
- `jansu-broker/tests/fetch.rs`
- `docs/compatibility/kafka-4.2-ledger.json`

## Files Changed

- `jansu-storage/src/service/list_offsets.rs` — replaced hardcoded `leader_epoch(Some(0))` with actual epoch from storage
- `jansu-storage/src/dynostore.rs` — added `leader_epoch_history` to `Meta`, initialized epoch 0 on topic creation, implemented `offset_for_leader_epoch` query
- `jansu-storage/src/lite.rs` — fixed pre-existing `sql_lookup` compile error in `leader_epoch_history` method
- `jansu-broker/src/service/storage.rs` — wired `OffsetForLeaderEpochService` into broker route stack
- `jansu-storage/tests/offset_for_leader_epoch.rs` — added 2 DynoStore epoch tests
- `docs/compatibility/kafka-4.2-ledger.json` — updated API 23 entry

## Tests Added

| Test | Location |
|------|----------|
| `dynostore_epoch_initialized_on_topic_creation` | `jansu-storage/tests/offset_for_leader_epoch.rs` |
| `dynostore_epoch_unknown_topition_returns_none` | `jansu-storage/tests/offset_for_leader_epoch.rs` |

## Verification Commands

```bash
# Full clean compile check
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase-08-final cargo check -p jansu-storage -p jansu-broker --no-default-features --features redlinedb,dynostore,slatedb --all-targets

# Broker-level fetch + list_offsets regression
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase-08-final cargo test -p jansu-broker --test fetch --test list_offsets --no-default-features --features redlinedb,dynostore,slatedb

# Storage epoch tests
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase-08-final cargo test -p jansu-storage --test offset_for_leader_epoch --no-default-features --features dynostore,redlinedb,slatedb
```

## Outcome

**SUCCESS** — All verification targets pass:

- Compile: Clean (no errors, only pre-existing warnings)
- Fetch tests: 6/6 non-PG pass
- ListOffsets tests: 9/9 non-PG pass
- Epoch storage tests: 3/3 pass (1 redlinedb + 2 dynostore)

Key accomplishments:
1. Removed hardcoded `leader_epoch(Some(0))` from `ListOffsetsService` — now resolves actual epoch from storage
2. Routed `OffsetForLeaderEpochService` (API 23) through the broker
3. Implemented in-memory leader epoch history in DynoStore (epoch 0 initialized on topic creation)
4. Fixed pre-existing `sql_lookup` compile error in `lite.rs`
5. Updated ledger: API 23 → `routed`, `partial`, with test proof

## Residual Risks

- **SlateDB epoch stub**: Returns `None` (epoch 0 fallback). Correct for single-leader deployments but won't track epoch boundaries for multi-leader scenarios.
- **Fetch response `current_leader`**: Still returns `None` on success path. Not phase-critical but could be enriched.
- **Pre-existing broker compile issues**: Some coordinator methods have unused imports and other pre-existing issues that are unrelated to Phase 08.
- **Postgres tests**: Blocked by local infrastructure (connection refused).

## Next Recommended Action

1. **SlateDB epoch history**: Add KV-based epoch tracking similar to DynoStore's BTreeMap approach.
2. **Fetch `current_leader`**: Populate with actual epoch on success response.
3. **API version advertisement**: Once differential client tests validate, consider advertising API 23 versions.
4. **Phase 09 or 11**: Proceed to next MASTER_PLAN priority.
