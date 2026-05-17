# Phase 08 - Fetch ListOffsets Leader Epoch

Parallelism: MCP-PARALLEL

Depends on: Phase 03 - Request Lifecycle No Hangs; Phase 06 - Storage Log Contract

Can run with: Phase 07, Phase 09, Phase 10, Phase 13, and Phase 14 after storage/log invariants are defined.

Goal: Make Fetch, ListOffsets, and OffsetForLeaderEpoch exact, including timestamp lookups, log start offsets, high watermark, last stable offset, leader epoch history, and fencing. API 23 must not be advertised until these semantics exist.

Current code anchors:
- `jansu-storage/src/service/fetch.rs` handles Fetch, high watermark, last stable offset, log start offset, and empty responses.
- `jansu-storage/src/service/list_offsets.rs` handles ListOffsets; partition leader epochs are resolved from storage (`leader_epoch_history` / `offset_for_leader_epoch`) with Kafka-style error and wire handling for mixed valid/invalid partitions (see `jansu-broker/tests/list_offsets.rs`).
- `jansu-storage/src/lib.rs` exposes `offset_stage`, `fetch`, and `list_offsets`.
- `jansu-storage/src/sql/list_latest_offset_*.sql` contains SQL offset query assets.
- `jansu-broker/tests/fetch.rs` and `jansu-broker/tests/list_offsets.rs` cover current behavior.
- The reverted `OffsetForLeaderEpoch` service showed a useful starting concept but was unsafe because it lacked leader-epoch history and truthful ApiVersions.

Implementation steps:
- Define Fetch and ListOffsets advertised version caps from Phase 04 client behavior.
- Add leader epoch storage to the Phase 06 log contract, including epoch start offsets, truncation boundaries, and recovery behavior.
- Implement `OffsetForLeaderEpoch` only after storage can answer historical epochs and current leader epoch validation.
- Return Kafka-equivalent errors for `FencedLeaderEpoch`, `UnknownLeaderEpoch`, `OffsetOutOfRange`, unknown topic/partition, invalid topic ID, and unsupported storage capability.
- Make Fetch long polling exact for `max_wait_ms`, `min_bytes`, `max_bytes`, `partition_max_bytes`, empty fetches, and cancellation.
- Implement incremental fetch sessions or lower advertised Fetch versions until sessions are supported.
- Complete topic ID behavior for modern Fetch/ListOffsets versions.
- Implement `read_committed` visibility with last stable offset and aborted transaction lists in coordination with Phase 12.
- Ensure ListOffsets supports earliest, latest, max timestamp, timestamp lookup, isolation level, and log start movement.

Tests:
- Add differential tests for consumer seek beginning/end/timestamp with Java client and librdkafka.
- Add Fetch timeout and no-hang tests from Phase 03 into the advertised version matrix.
- Add leader epoch history tests before routing API 23.
- Add negative tests for stale/current leader epoch, unknown epoch, fenced epoch, and truncation-like scenarios.
- Add storage conformance tests for timestamp lookup, high watermark, last stable offset, and log start offset.

Acceptance gate: Consumers seek, fetch, timestamp-search, and leader-epoch paths match Kafka for every advertised version; `OffsetForLeaderEpoch` is advertised only after true leader-epoch semantics are implemented.

Do not do:
- Do not re-add the unsafe `OffsetForLeaderEpochRequest::KEY` route that only returns high watermark and echoes the requested epoch.
- Do not advertise Fetch versions that require incremental sessions unless sessions are implemented or proved unnecessary for those clients.
- Do not fake leader epoch history with a constant zero.
- Do not return empty success for cases Kafka treats as fencing, unknown epoch, or offset errors.

Fresh session handoff: Start by adding leader epoch requirements to the storage/log contract, then implement ListOffsets and Fetch fixes. Leave API 23 unadvertised until historical epoch tests pass.

---

## Supplement 2026-05-04 — Implementation status (append-only)

- **ListOffsets leader epoch:** No longer a hardcoded constant; broker/storage paths use epoch history where implemented (DynoStore, RedlineDB, SlateDB patterns in `jansu-storage`; Postgres follows the same SQL assets where wired). See `phase-logs/08-fetch-list-offsets-leader-epoch.md.log` for session history.
- **API 23 (`OffsetForLeaderEpoch`):** Routed through the broker stack but **not** advertised in ApiVersions until ledger `semantic_status`, storage certification, and failure-mode proofs catch up (`docs/compatibility/kafka-4.2-ledger.json` api_key 23).
- **Flexible protocol:** `jansu-sans-io` encoder now emits Kafka JSON **defaults** for non-nullable `Option` fields on flexible messages when serde serializes `None` (e.g. `CurrentLeaderEpoch` default `-1` on ListOffsets / Fetch partition requests), so request frames round-trip on the wire without requiring callers to set `Some(-1)` manually.
- **Fetch byte-accounting progress:** `FetchService` now stops polling without sleeping once a response satisfies `min_bytes`, accounts bytes per poll attempt instead of cumulatively across empty retries, and caps each partition by `partition_max_bytes` before spending global `max_bytes`. Focused dynostore storage tests cover the no-extra-wait and partition limit behavior; broker Fetch passes across dynostore, redlinedb, and slatedb.
- **Residual (unchanged acceptance gate):** read_committed + LSO + aborted transactions, incremental fetch sessions vs version cap, topic ID behavior, broader max-bytes parity, truncation/recovery proofs, Java/librdkafka differential seeks, and API 23 advertisement remain open until tests and ledger rows justify each claim.

---

## Supplement 2026-05-03 — Agent routing (next phase)

With **Phase 04**, **Phase 07**, and **Phase 10** marked complete in `MASTER_PLAN.md`, the next **logical** execution target for Kafka-parity work is **this phase (08)**—priority queue position **2**, manifest status **in-progress**, and `AUDIT-004` still open. **Phase 09** may proceed in parallel only when file ownership stays disjoint from Fetch/ListOffsets/epoch storage (`MASTER_PLAN.md` parallel rules). Phase **11** must wait until Produce (07) and chosen 08/09 surfaces are stable per plan.

## Supplement 2026-05-03 — Differential evidence (ListOffsets + Fetch consume)

- **Harness:** `jansu-broker/tests/differential_lab.rs::differential_listoffsets_latest_and_fetch_consume_after_produce` (requires `JANSU_DIFFERENTIAL=1` and Kafka 4.2 baseline per Phase 04).
- **Checks:** After identical librdkafka Produce to partition 0, wire **ListOffsets v9** `Latest` high-watermark must match Kafka vs Jansu; librdkafka **StreamConsumer** assigned to beginning must return identical payloads in order (Fetch path).
- **Ledger:** `docs/compatibility/kafka-4.2-ledger.json` API keys **1** and **2** include this test under `differential_tests`; APIs remain **unadvertised** — acceptance gate and “do not advertise Fetch versions requiring incremental sessions” rules unchanged.

## Supplement 2026-05-04 — Differential ListOffsets Earliest

- The same `differential_listoffsets_latest_and_fetch_consume_after_produce` workload now asserts **ListOffsets v9 Earliest** returns the log start offset matching Kafka (expected **0** after produce from base offset 0), in addition to **Latest** HWM and librdkafka consume parity. Wire helper refactored to `list_offsets_partition_offset` with explicit correlation ids per request. The test is **not** `#[ignore]` — it runs whenever `JANSU_DIFFERENTIAL=1` (otherwise it skips immediately like other differential tests).

## Supplement 2026-05-05 — Produce differential default run

- **`differential_produce_round_trip_for_advertised_api`** is no longer `#[ignore]`; it matches Phase 07 advertised Produce and the ledger API key **0** `differential_tests` row. With `JANSU_DIFFERENTIAL=1`, one `cargo test -p jansu-broker --test differential_lab` run now includes ApiVersions, Metadata, Produce librdkafka round-trip, and ListOffsets/Fetch read evidence (except the intentionally ignored full lifecycle placeholder).

## Supplement 2026-05-05 — Fetch ReadCommitted (non-txn) proof

- **`jansu-storage/tests/fetch.rs::phase08::fetch_read_committed_matches_uncommitted_when_no_transactions`** asserts `FetchService` returns identical record counts for **ReadUncommitted** vs **ReadCommitted** on dynostore memory when there are no open transactions (LSO == HWM). This does **not** certify transactional read_committed or aborted-txn filtering (Phase 12 / broader Phase 08 backlog).

## Supplement 2026-05-04 — ListOffsets returned-offset leader epoch proof

- `ListOffsetsService` now uses `leader_epoch_for_offset` for successful responses, so the response `leader_epoch` reflects the epoch active at the returned offset instead of always reporting the current partition epoch.
- `jansu-storage/tests/list_offsets.rs::response_frame_round_trips_for_produced_leader_epoch` now asserts both sides of the epoch boundary after producing epoch 0 and epoch 1 batches: `Latest` returns offset `2` with leader epoch `1`, while `Earliest` returns offset `0` with leader epoch `0`.
- This tightens API key **2** unit proof without changing ApiVersions advertisement; ListOffsets remains unadvertised until read_committed/LSO, truncation/recovery, and broader client/differential proof satisfy the acceptance gate.
