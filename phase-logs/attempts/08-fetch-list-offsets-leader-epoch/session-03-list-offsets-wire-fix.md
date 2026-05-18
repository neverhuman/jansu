# Attempt Log — Phase 08 Session 3

- **Agent**: Antigravity/Gemini
- **Prompt**: Progress on MASTER_PLAN and audit — fix broker ListOffsets wire failures
- **Phase Or Audit Item**: Phase 08 / AUDIT-004

## Files Read

- `MASTER_PLAN.md`
- `AUDIT.md`
- `phase-logs/index.json`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log`
- `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- `tips/jansu_phases_tips/phase-08/tip1.txt` through `tip5.txt`
- `jansu-storage/src/service/list_offsets.rs`
- `jansu-storage/src/service/leader_epoch.rs`
- `jansu-storage/src/lib.rs` (ListOffsetResponse)
- `jansu-storage/src/dynostore.rs` (leader_epoch_history, list_offsets)
- `jansu-broker/tests/list_offsets.rs`
- `jansu-storage/tests/list_offsets.rs`

## Files Changed

- `jansu-storage/src/service/list_offsets.rs` — full rewrite (BTreeSet → planned-slot topology preservation)
- `jansu-storage/src/service/leader_epoch.rs` — added `current_leader_epoch_error`, `leader_epoch_for_offset`
- `jansu-broker/tests/list_offsets.rs` — added missing `.current_leader_epoch(Some(-1))`
- `jansu-storage/tests/list_offsets.rs` — updated assertions for correct non-existent topic behavior
- `AUDIT.md` — updated AUDIT-004
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log` — appended session 3
- `phase-logs/index.json` — removed resolved residual risk

## Tests Added

- No new test files, but corrected existing tests to match Kafka-correct behavior

## Verification Commands

```bash
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-broker --test list_offsets --no-default-features --features dynostore -- mixed_partition_errors leader_epoch_tracks_produced_epoch --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-broker --test list_offsets --no-default-features --features dynostore -- --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-broker --test list_offsets --no-default-features --features redlinedb,dynostore,slatedb -- --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-storage --test offset_for_leader_epoch --no-default-features --features dynostore,redlinedb,slatedb -- --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-storage --test list_offsets --no-default-features --features dynostore,redlinedb,slatedb -- --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-broker --test fetch --no-default-features --features dynostore -- --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo test -p jansu-broker --test compatibility_contract --all-features -- --nocapture
env CARGO_TARGET_DIR=/tmp/jansu-verify-phase08-fix cargo check -p jansu-storage -p jansu-service -p jansu-broker --all-features --all-targets
```

## Outcome

**SUCCESS** — Both previously failing broker tests now pass:
- `in_memory::mixed_partition_errors`: ✅ (was assertion failure `left: 3, right: 0`)
- `in_memory::leader_epoch_tracks_produced_epoch`: ✅ (was `KafkaProtocol(UnexpectedEof)`)

Full multi-backend verification: 21/21 broker ListOffsets, 7/7 storage epoch, 4/4 storage list_offsets, 2/2 broker Fetch, 15/15 compatibility contract.

## Residual Risks

- API 23 remains unadvertised until differential/client proof
- Fetch long-polling byte accounting not hardened
- Leader epoch truncation/recovery unproven
- Read-committed transaction integration untested

## Next Recommended Action

1. Harden Fetch long-polling: fix byte accumulation bug, enforce `partition_max_bytes`
2. Add leader epoch truncation/recovery conformance tests
3. Build Phase 04 differential test harness for ListOffsets/Fetch/OffsetForLeaderEpoch
4. Advertise API 23 only after differential proof exists
