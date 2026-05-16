# veox-native feature request for Jansu

The xdoug native runtime now uses an in-process event bus for direct-test mode.
This document tracks the upstream Jansu work needed to make that bus the real
embedded path.

Required work:

1. No-SQL memory-only feature profile.
   - `cargo build --bin jansu --no-default-features --features dynostore`
   - no Postgres/libSQL/SQLite/Turso unless explicitly enabled

2. Embeddable in-process broker.
   - start a memory broker from Rust without shelling out
   - listener port `0` returns the actual bound address
   - graceful shutdown via cancellation token
   - memory topics drop when the broker/session drops

3. Advertised API coverage.
   - Fetch
   - ListOffsets
   - CreateTopics
   - OffsetCommit/Fetch
   - FindCoordinator
   - JoinGroup
   - SyncGroup
   - Heartbeat
   - LeaveGroup

4. Veox event-bus contract.
   - publish event
   - consume from offset
   - consumer-group worker consumption
   - reconnect/replay
   - bounded memory behavior

5. Standard-client proof.
   - add tests against `memory://`
   - prove topic create, produce, fetch, list offsets, and consumer-group basics

Pending work discovered during xdoug validation:

- Clean up the dynostore-path warnings so the memory-only proof path is
  warning-free. The current validation still emits dead-code warnings in
  `jansu-storage/src/proxy.rs` and unused-import warnings in the broker
  coordinator/administrator modules.
- Add explicit CI coverage for `--no-default-features --features dynostore`
  so the memory-only build stays green as the default feature set evolves.
- Add a dedicated embedded-broker regression test that exercises broker start,
  topic creation, produce/fetch, consumer-group basics, and shutdown without
  shelling out.
- Timestamp-based lookup now resolves explicit produced record timestamps in
  the in-memory path, including the end-of-log after-last case, and the
  non-Postgres local profiles (`libsql` and `slatedb`) now match the same
  dynostore contract.

Acceptance target:
- the native runtime can depend on Jansu as its canonical embedded event bus
  without shelling out or requiring a Postgres/SQLite-backed sidecar
