# Architecture

Jansu is a Rust Kafka-compatible broker workspace. Protocol encoding and
decoding live in `jansu-sans-io`, broker request routing lives in
`jansu-broker`, durable state lives behind `jansu-storage`, and schema payload
handling lives in `jansu-schema`.

Compatibility truth is ledger-backed in `docs/compatibility/kafka-4.2-ledger.json`.
