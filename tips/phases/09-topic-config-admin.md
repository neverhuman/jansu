# Phase 09 - Topic Config Admin

Parallelism: MCP-PARALLEL

Depends on: Phase 05 - Route Coverage Safe Errors; Phase 06 - Storage Log Contract

Can run with: Phase 07, Phase 08, Phase 10, Phase 13, and Phase 14 after safe route behavior exists.

Goal: Finish topic, config, and AdminClient semantics so Kafka CLI and AdminClient can manage Jansu without special cases.

Current code anchors:
- `jansu-storage/src/service/create_topics.rs`, `delete_topics.rs`, `delete_records.rs`, `describe_configs.rs`, `incremental_alter_configs.rs`, `describe_topic_partitions.rs`, `list_partition_reassignments.rs`, and `metadata.rs`.
- `jansu-broker/src/service/storage.rs` wires current admin routes.
- `jansu-broker/tests/topic.rs`, `metadata.rs`, `describe_configs.rs`, and `policy_compact_delete.rs` cover existing admin behavior.
- `justfile` includes Kafka CLI targets for topics, configs, offsets, produce, consume, and groups.
- `README.md` documents Kafka CLI workflows and the current "all brokers are node 111" simplification.

Implementation steps:
- Certify CreateTopics, DeleteTopics, Metadata, DescribeConfigs, IncrementalAlterConfigs, DescribeTopicPartitions, ListPartitionReassignments, and DeleteRecords by API version.
- Add or complete CreatePartitions, legacy AlterConfigs if required by clients, DeleteRecords edge cases, config synonyms, config defaults, dynamic configs, validation-only behavior, and unknown config errors.
- Define exact behavior for replication factor and assignments under Jansu's stateless/storage-backed profiles.
- Implement topic ID behavior consistently across Metadata, Fetch, ListOffsets, DeleteTopics, and admin APIs.
- Make config support explicit for cleanup policy, retention, compaction, min.insync.replicas, max message bytes, timestamp type, segment-like compatibility aliases, and unsupported JVM-specific configs.
- Add metadata refresh tests for topic creation, deletion, partition changes, and storage-engine capability changes.
- Wire route coverage and ledger status for AdminClient-probed APIs.

Tests:
- Add Kafka CLI tests for `kafka-topics`, `kafka-configs`, `kafka-get-offsets`, and `kafka-reassign-partitions` where targeted.
- Add Java AdminClient differential tests for topic lifecycle, configs, validation-only, and delete records.
- Add negative tests for invalid topic names, invalid partitions, invalid replication factors, duplicate topics, unknown configs, unsupported configs, and unauthorized operations once Phase 14 lands.
- Add storage conformance tests for config-driven retention and compaction behavior that overlaps Phase 13.

Acceptance gate: Kafka CLI and Java AdminClient can create, describe, alter, delete, partition, configure, and inspect Jansu topics without Jansu-specific flags or client workarounds.

Do not do:
- Do not accept unknown configs as successful if Kafka would reject or report them.
- Do not silently ignore replication factor or assignment inputs without a documented profile rule.
- Do not let topic IDs drift across metadata APIs.
- Do not implement AdminClient happy paths without exact error behavior.

Fresh session handoff: Start with the AdminClient operation matrix, then fill gaps in service modules one API at a time. Update the Phase 01 ledger as each admin path moves from safe-error to supported.

## Supplement - 2026-05-16

- DynoStore topic config writes now reject unsupported topic keys with `InvalidRequest` rather than persisting them.
- The create-topics and incremental-alter-configs regressions now use `cleanup.policy` as the supported key and cover the negative path for `x.y.z`.

## Supplement - 2026-05-16 2

- DynoStore `DescribeConfigs` now emits default topic configs for `cleanup.policy` and `retention.ms`.
- Broker describe-config tests were updated to expect the default config shape when a topic has no explicit config overrides.

## Supplement - 2026-05-16 3

- DynoStore `DescribeConfigs` now respects requested `configuration_keys` for topics.
- Broker describe-config tests now cover both the full default topic config shape and the filtered single-key response for `cleanup.policy`.

## Supplement - 2026-05-16 4

- `DescribeConfigs` now surfaces a Kafka-style synonym for `cleanup.policy` when `include_synonyms=true`.
- Synonym coverage for `retention.ms` and broader config aliases remains open.

## Supplement - 2026-05-16 5

- `DescribeConfigs` now surfaces a Kafka-style synonym for `retention.ms` when `include_synonyms=true`.
- Remaining alias coverage is now limited to other topic config names beyond `cleanup.policy` and `retention.ms`.

## Supplement - 2026-05-16 6

- **Centralized config defaults**: New `topic_config_defaults.rs` module in the
  service layer defines 22 Kafka-standard topic config defaults, types, and
  synonym mappings. All backends now return identical default config sets via
  service-layer merging.
- **Expanded allowlist**: `Meta::supports_topic_config()` in DynoStore now
  accepts all 22 standard Kafka topic config keys for `CreateTopics` and
  `IncrementalAlterConfigs`. Configs are stored but behavioral enforcement is
  deferred to their owning phases.
- **Cross-backend parity**: PG and limbo backends previously returned empty
  configs for topics with no overrides. The service layer now merges defaults
  on top, ensuring consistent responses across all 4 backends.
- **Error code fix**: DynoStore `DescribeConfigs` now returns
  `UnknownTopicOrPartition` for non-existent topics (was `ErrorCode::None`).
- **Synonym expansion**: All 22 topic configs now have proper broker-level
  synonym mappings (e.g., `segment.bytes` → `log.segment.bytes`).
- **Key filtering**: Moved from storage layer to service layer, applied after
  default merging so filtered requests work identically across all backends.

