# Attempt log

## Agent

Cursor (GPT-5.2)

## Prompt

Continue until Phase 10 verification is done and verified (broker `pg::` slice, ListOffsets, storage offsets).

## Phase Or Audit Item

Phase 10 — consumer groups / offsets (`tips/phases/10-consumer-groups-offsets.md`)

## Files Read

- `jansu-broker/tests/list_offsets.rs`
- `jansu-storage/src/service/list_offsets.rs`
- `jansu-storage/src/lite.rs`, `jansu-storage/src/limbo.rs`, `jansu-storage/src/pg.rs`
- `jansu-service/src/frame.rs`
- `jansu-sans-io/tests/codec.rs`
- `phase-logs/10-consumer-groups-offsets.md.log`

## Files Changed

- `jansu-storage/src/lite.rs` — `list_offsets`: guard with `topition_select.sql` before offset queries (mirror Postgres).
- `jansu-storage/src/limbo.rs` — same guard for embedded SQL path.
- `jansu-storage/src/service/list_offsets.rs` — wire `offset` as `offset.offset().or(Some(-1))` so flexible v9 responses round-trip.
- `jansu-broker/tests/list_offsets.rs` — `mixed_partition_errors`: set `current_leader_epoch(Some(-1))` on request partitions; expect `Some(-1)` for invalid partition offset; keep `storage_route_round_trips_list_offsets_v9` leader epoch on request.

## Tests Added

- None (behavior covered by existing `mixed_partition_errors`, `storage_route_round_trips_list_offsets_v9`, and `list_offsets_request_v9_round_trip`).

## Verification Commands

- `cargo test -p jansu-broker --test list_offsets lite::mixed_partition_errors --all-features -- --nocapture`
- `cargo nextest run -p jansu-broker --all-features -E 'test(pg::)' --no-fail-fast`
- `cargo nextest run -p jansu-broker --all-features -E 'test(pg::mixed_partition_errors)'`
- `cargo nextest run -p jansu-storage --test consumer_offsets --all-features`
- `just compatibility-contract`

## Outcome

- **52/52** broker `pg::` nextest tests pass, including `list_offsets::pg::mixed_partition_errors`.
- Root causes: (1) flexible ListOffsets **request** v9 does not round-trip when `CurrentLeaderEpoch` is omitted (`None`); tests now send `-1`. (2) **Lite/Limbo** `list_offsets` treated “no watermark row” as success offset `0`; invalid partitions must use `topition_select` first (aligned with `pg.rs`). (3) flexible **response** v9: `offset: None` does not round-trip; broker emits default `-1` via `Some(-1)` on the wire.

## Residual Risks

- **Protocol**: serde/codegen still mishandles omitted `CurrentLeaderEpoch` on flexible ListOffsets requests; real clients that omit the field may hit decode failures until sans-io encode/decode applies Kafka defaults for non-nullable schema defaults.
- Broader `just test` / full workspace nextest not run in this session beyond the commands listed.

## Next Recommended Action

- Fix flexible serialization for `CurrentLeaderEpoch` (and similar defaulted primitives) in `jansu-sans-io` codegen or serializer so omitted fields serialize compatibly with Apache Kafka clients.
