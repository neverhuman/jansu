# Contracts

Jansu does not keep OpenAPI-style contracts in this directory. The active
contracts are:

- Kafka wire message descriptors: `jansu-sans-io/message/`
- Generated protocol code: `jansu-sans-io/src/`
- Compatibility claim ledger: `docs/compatibility/kafka-4.2-ledger.json`
- Compatibility contract test: `jansu-broker/tests/compatibility_contract.rs`

Run `just compatibility-contract` after changing any advertised API version,
message generation path, or compatibility ledger row.
