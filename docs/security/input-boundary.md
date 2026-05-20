# Input Boundaries

This document routes external input surfaces to validators and proof lanes.

## Kafka Wire Bytes

Owner: `jansu-sans-io`.

Request and response bytes are decoded through generated message metadata. The
proof lane is `cargo test -p jansu-sans-io --all-features` plus the broker
compatibility contract when advertised API behavior changes.

## Record Batches

Owner: `jansu-sans-io`.

Record batch decoding validates framing, compression attributes, CRCs, and
bounded lengths before storage code observes records. Fuzz targets under
`fuzz/` provide adversarial decode coverage.

## Schema Payloads

Owner: `jansu-schema`.

Avro, JSON, Protobuf, Delta, Iceberg, and Parquet conversions accept producer
payloads and schema assets. The proof lane is `cargo test -p jansu-schema
--all-features`; schema asset path changes must also run a focused regression.
Test-only SQL used by schema conversions must use fixed table names or validated
identifiers before calling query engines.
Delta Lake generated-column rewrites must use typed projection APIs or parsed
expressions from allowlisted schema metadata; do not interpolate a `SELECT`
string from untrusted column lists.

## SQL Assets

Owner: `jansu-storage`.

Runtime database access is centralized in storage backends. SQL assets under
`jansu-storage/src/sql/` and `jansu-storage/src/lite/` must use bound
parameters for cluster, topic, group, user, transaction, and retention inputs.
Destructive SQL proof is tracked in `docs/db/destructive-delete-proof.md`.
