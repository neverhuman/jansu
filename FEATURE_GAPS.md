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
- Resolved: backend-specific end-of-log timestamp behavior for `redlinedb` and
  `slatedb` now matches the dynostore contract on the after-last timestamp
  case.
- Resolved: `DescribeConfigs` now returns 22 Kafka-standard topic config
  defaults (cleanup.policy, retention.ms, segment.bytes, max.message.bytes,
  min.insync.replicas, compression.type, etc.) via centralized service-layer
  defaults. All backends (dynostore, slatedb, redlinedb, pg) return identical
  config sets.
- Resolved: Synonym expansion now covers all 22 topic config keys with
  proper broker-level alias names (e.g., segment.bytes → log.segment.bytes).
- Resolved: Cross-backend DescribeConfigs parity — PG and limbo backends
  now return full default config sets via the service layer, fixing the gap
  where they previously returned empty configs for topics with no overrides.
- Resolved: IncrementalAlterConfigs now accepts all 22 standard Kafka
  topic config keys in DynoStore (stored but enforcement deferred).
- Resolved: Non-existent topics now return `UnknownTopicOrPartition` in
  DynoStore DescribeConfigs (was incorrectly returning `ErrorCode::None`).
- Note: Items 1–6 above (embedding, veox, no-SQL profile) are Jansu-native
  embedding features, not Kafka protocol parity gaps.
