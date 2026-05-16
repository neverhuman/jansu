# FEATURE GAPS

Append-only tracker for gaps surfaced by veox-native integration work.

## 2026-05-15

- Need a no-SQL memory-only feature profile for native embedding.
- Need an embeddable in-process broker that can start from Rust without
  shelling out.
- Need listener-port-0 support that returns the actual bound address.
- Need graceful shutdown via cancellation token and memory-topic cleanup when
  the broker/session drops.
- Need advertised API coverage for:
  - Fetch
  - ListOffsets
  - CreateTopics
  - OffsetCommit/Fetch
  - FindCoordinator
  - JoinGroup
  - SyncGroup
  - Heartbeat
  - LeaveGroup
- Need a veox event-bus contract for publish, offset consumption,
  consumer-group worker consumption, reconnect/replay, and bounded memory.
- Need standard-client proof tests against `memory://` for produce/fetch/list
  offsets/topic create/consumer-group basics.
- Resolved: Kafka-equivalent timestamp seek / lookup parity for explicitly
  produced record timestamps now includes the end-of-log after-last case in the
  in-memory path.
- Resolved: backend-specific end-of-log timestamp behavior for `libsql` and
  `slatedb` now matches the dynostore contract on the after-last timestamp
  case.
