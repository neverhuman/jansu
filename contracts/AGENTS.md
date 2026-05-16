<!-- jankurai:owner: contracts -->
<!-- jankurai:proof-lane: contracts-audit -->
<!-- jankurai:expiry: 2026-07-01 -->

# Contract Artifacts Ownership

This file declares ownership of the generated contract artifacts that live
under `contracts/`. The directory holds the Avro, JSON Schema, and Protocol
Buffers descriptors that define every cross-process boundary Jansu exposes
to producers, consumers, schema registries, and lake-side readers. Because
these files are consumed by external systems, they are treated as a
generated zone: the canonical source is the upstream definition or the
build script that produces them, never the file in the working tree.

## Profile

- Profile name: `contracts-artifacts`
- Surface root: `contracts/`
- Owner: Protocol Engineering (`@contracts`)
- Proof lane: `contracts-audit`
- Trust boundary: external clients and downstream lake readers
- Review cadence: every release-candidate cut, and on every upstream
  Apache Kafka or schema-registry version bump.

## Artifact Directories

The `contracts/` tree holds the following artifact families. Each family
has its own generator, its own naming convention, and its own consumer.
None of them may be hand-edited:

- `contracts/avro/` - Avro `.avsc` descriptors. These are the on-wire
  schemas for Avro-encoded topics and must round-trip with the Arrow
  encoder at `jansu-schema/src/avro/arrow.rs` and the Avro encoder at
  `jansu-schema/src/avro.rs`.
- `contracts/json/` - JSON Schema `.json` descriptors. These define the
  JSON-encoded topic payloads consumed by the JSON encoder at
  `jansu-schema/src/json/arrow.rs`.
- `contracts/proto/` - Protocol Buffers `.proto` files. These define the
  Protobuf-encoded topic payloads consumed by the Protobuf encoder at
  `jansu-schema/src/proto/arrow.rs`.
- `contracts/kafka/` - The Kafka API message descriptors. These mirror the
  upstream Apache Kafka JSON descriptors at
  `jansu-sans-io/message/*.json` and feed the generator at
  `jansu-sans-io/build.rs`. They are tracked separately so that contract
  audits can diff the broker's protocol against the upstream definition
  on every upgrade.
- `contracts/lake/` - Iceberg and Delta Lake table-level descriptors used
  by the lake exporters at `jansu-schema/src/lake/`.

Files outside these directories are not in the `contracts-artifacts`
profile. Sample schemas used by tests and local development belong under
`etc/schema/`; production-shape contracts belong under `contracts/`.

## Owner

- Group: Protocol Engineering
- GitHub handle placeholder: `@contracts`
- Escalation path: Protocol Engineering on-call, then the release
  manager listed in `docs/release/release-readiness.md`.

The handle `@contracts` is a placeholder. When a real GitHub team is
provisioned, the handle here and in `agent/audit-policy.toml` must change
together.

## Proof Lane

The proof lane is `contracts-audit`. Every change to a file in scope must
produce evidence that the file was generated, not authored by hand, and
that the change does not break backward compatibility for any consumer
already in production. Lane requirements:

1. The pull request description names the generator command that produced
   the change (for example `just regenerate-contracts` or the explicit
   `cargo run -p ...` invocation).
2. Re-running the named generator on the merge base produces an identical
   working tree to the merge commit, modulo whitespace.
3. For Avro and Protobuf descriptors, the change is checked against the
   in-tree schema-evolution test (`jansu-schema/src/avro.rs`,
   `jansu-schema/src/proto.rs`) that exercises producer-then-consumer
   round trips for the previous and the new schema.
4. For Kafka API descriptors, the change is checked against the
   differential-Kafka lab harness invoked from
   `.github/workflows/differential-kafka-lab.yml`.
5. For lake descriptors, the change is checked against the lake export
   integration tests under `jansu-schema/src/lake/`.

Pull requests that cannot satisfy all five points are blocked.

## Forbidden Patterns

Reviewers must reject any pull request that:

- **Hand-edits a generated zone.** Manual edits to any file under
  `contracts/avro/`, `contracts/json/`, `contracts/proto/`,
  `contracts/kafka/`, or `contracts/lake/` are forbidden. The
  authoritative reference for the generated-zones list is
  `agent/generated-zones.toml`; if a path appears in that file, it is a
  generated zone and may only be modified by re-running the generator.
- **Renames a contract without a deprecation window.** Renaming a top-level
  Avro record, a JSON Schema `title`, a Protobuf `message`, or a Kafka
  API name immediately breaks every consumer that pinned the previous
  identifier. Renames require a parallel-publish window in which both the
  previous and the new identifier are emitted, with the previous
  identifier marked for sunset in a follow-up release.
- **Lowers a field's type width.** Going from `int64` to `int32`, from
  `string` to a constrained enum, or from a nullable to a non-nullable
  field is a breaking change. The lane fails such pull requests.
- **Adds a required field without a default.** Required-without-default
  fields break every producer that has not yet been redeployed. Use an
  optional field, or use a required field with a sentinel default, until
  every producer is known to have been redeployed.
- **Embeds environment-specific values.** Hostnames, account identifiers,
  bucket names, and other deployment-shaped strings must live in
  configuration, not in a contract artifact.
- **Bypasses the build cache.** Contracts must be regenerated from a
  clean tree; partial regeneration that mixes outputs from two generator
  versions is forbidden.

## Required Patterns

- Every contract file begins with a generator banner that names the
  generator, the generator version, and the upstream source revision.
- Every Avro `.avsc` carries a `namespace` that begins with `io.jansu.`.
- Every Protobuf `.proto` carries an explicit `syntax = "proto3";` line.
- Every Kafka descriptor carries `validVersions` and `flexibleVersions`
  fields that exactly mirror the upstream definition.

## Negative Tests

The `contracts-audit` lane runs the following negative checks and
confirms each one fails with the matching code:

1. Touching `contracts/avro/SampleEvent.avsc` by hand (without rerunning
   the generator) must fail with `hand_edit_in_generated_zone`.
2. Renaming the record `SampleEvent` to `SampleEventV2` without a
   parallel-publish window must fail with `breaking_rename`.
3. Adding a required `int64 customer_id` field with no default must fail
   with `required_field_without_default`.

## Backward Compatibility Rules

The `contracts-audit` lane enforces these rules for each artifact family:

- Avro: full compatibility (`both`). Readers using the previous schema can
  decode data written with the new schema, and vice versa.
- JSON Schema: superset compatibility. The new schema accepts every
  document accepted by the previous schema.
- Protobuf: field-number-stable compatibility. Field numbers are never
  reassigned; reserved numbers are documented in the artifact.
- Kafka API: protocol version compatibility. New API versions are
  additive only, and every previous version remains decodable.
- Lake: column-add-only compatibility. Columns may be added, never
  removed or retyped.

## Cross-References

- `agent/generated-zones.toml` is the authoritative list of generated
  zones. If a path under `contracts/` is missing from that file, the
  audit lane fails closed.
- `jansu-sans-io/build.rs` is the generator for the Kafka API
  descriptors.
- `jansu-schema/src/{avro,json,proto}.rs` and their `arrow.rs` siblings
  are the round-trip engines exercised by the contracts lane.
- `docs/release/release-readiness.md` ties the contracts lane into the
  launch gate so a release cannot ship with a red lane.

## Change Control

Changes to this file require review by a Protocol Engineering owner plus
a green `contracts-audit` lane. Renames of generator outputs must also be
reflected in `agent/generated-zones.toml` in the same change set.
