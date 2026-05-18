# Differential Kafka Lab

Phase: 04
Audit item: AUDIT-012
Baseline: Apache Kafka 4.2.0
Kafka image: `apache/kafka:4.2.0`

## Purpose

The differential lab compares Jansu against a real Kafka 4.2 broker for every API
Jansu advertises in `docs/compatibility/kafka-4.2-ledger.json`.

The lab has two tiers:

1. **Contract tier**: always runs without external services via `compatibility_contract.rs`.
2. **External differential tier**: runs only with `JANSU_DIFFERENTIAL=1` and requires Docker or a supplied Kafka bootstrap.

## Current Advertised API Scope

Jansu currently advertises:

- `ApiVersionsRequest` (v0..=4)
- `MetadataRequest` (v12)
- `ProduceRequest` (v0..=11)

With `JANSU_DIFFERENTIAL=1`, the external tier runs **`differential_produce_round_trip_for_advertised_api`** (librdkafka Produce vs Kafka 4.2) in addition to ApiVersions/Metadata checks.

Fetch, ListOffsets, consumer groups, idempotence, and transactions remain route-level
or unadvertised until their owning phases promote them; selected ListOffsets/Fetch
evidence runs via `differential_listoffsets_latest_and_fetch_consume_after_produce` as
documented in Phase 08 / `AUDIT-004`.

**Evidence-only differential workloads (unadvertised APIs):** Phase 08 may add
`differential_lab` tests that compare Kafka 4.2 and Jansu for wire or client
behavior **without** adding those APIs to `approved_advertised_versions`. Failures
are owned by the phase/audit item for that API (for example `AUDIT-004`), not by
Phase 04’s advertised-API contract. The ListOffsets v9 **latest** + **earliest** + librdkafka consume-after-produce
check lives in `differential_listoffsets_latest_and_fetch_consume_after_produce`.

## Commands

### Run the contract tier (no Docker needed)

```sh
cargo test -p jansu-broker --test compatibility_contract --all-features -- --nocapture
```

### Run the external Kafka 4.2 differential tier

```sh
JANSU_DIFFERENTIAL=1 \
CARGO_TARGET_DIR=/tmp/jansu-verify-phase04-differential \
cargo test -p jansu-broker --test differential_lab --all-features -- --nocapture
```

When the harness auto-starts Kafka, each test gets an isolated Docker Compose
project, container name, and free localhost port so Rust's default parallel test
execution does not collide on Docker resources. If an auto-start attempt fails,
the retry allocates a fresh sandbox. A forced `JANSU_DIFF_KAFKA_PORT` is reused
only because the caller explicitly requested that fixed port.

### Use an already-running Kafka broker

```sh
JANSU_DIFFERENTIAL=1 \
JANSU_DIFF_KAFKA_BOOTSTRAP=127.0.0.1:19092 \
cargo test -p jansu-broker --test differential_lab --all-features -- --nocapture
```

### Start/stop the Kafka 4.2 reference broker manually

```sh
just differential-kafka-up
just differential-kafka-down
```

### Run Kafka CLI fixtures

```sh
scripts/differential/kafka-cli-fixtures.sh 127.0.0.1:19092 kafka42 target/differential/cli
```

## Artifact Shape

Artifacts are written under `target/differential/` unless `JANSU_DIFF_ARTIFACT_DIR` is set.

Every artifact contains:

* `schema_version`
* workload name
* Kafka bootstrap address
* Jansu bootstrap address (where relevant)
* normalized observations

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `JANSU_DIFFERENTIAL` | unset | Set to `1` to enable external differential tests |
| `JANSU_DIFF_KAFKA_BOOTSTRAP` | auto-start via Docker | Skip Docker; connect to existing Kafka |
| `JANSU_DIFF_KAFKA_PORT` | free localhost port | Optional fixed port for a Docker-managed Kafka instance; use serial test execution when forcing one port |
| `JANSU_DIFF_KAFKA_CONTAINER` | generated per test | Docker container name for a Docker-managed Kafka instance |
| `JANSU_DIFF_ARTIFACT_DIR` | `target/differential` | Output directory for JSON artifacts |

## Completion Rule

Phase 04 is complete when:

* Kafka 4.2 can be started locally or supplied externally.
* Jansu can be started side-by-side.
* Advertised API behavior is compared against Kafka 4.2.
* Results are written as artifacts.
* Ledger proof points to the differential tests.
* `AUDIT-012` is resolved.

## Ownership Rule

Phase 04 owns the differential infrastructure, artifact schema, Kafka 4.2 reference
target, and compatibility-ledger proof plumbing.

A semantic mismatch for an unadvertised API does not block Phase 04 completion. It
must be recorded as evidence for the owning phase. An advertised API mismatch does
block Phase 04 and must fail the differential lab.
