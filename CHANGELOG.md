# Changelog

## [0.6.1] - 2026-05-17

First semver-tagged release of jansu after `version = "0.6.0"` shipped on
main without a corresponding git tag. Adds a new crate for in-process
embedding.

### Added

- **`jansu-embedded`** — new workspace crate. See its `src/lib.rs` for
  the full surface; in summary: `EmbeddedBroker::send / consumer /
  create_topic`, a fluent `EmbeddedBrokerBuilder`, and an
  `EmbeddedRecord` value type. No TCP listener — the broker calls the
  `Storage` services directly with in-memory storage by default.
  14 integration tests + 1 example.

### Changed

- Workspace `version` bumped 0.6.0 → 0.6.1 (along with all sibling crate
  version pins under `[workspace.dependencies]`).

### Notes for downstream consumers

- `jansu-embedded` is additive; existing `jansu-broker` (TCP) consumers
  are unaffected.
- Suitable for single-binary control-plane deployments where producers
  and consumers live in the same process and a TCP listener is unwanted.
  For multi-process or remote consumers, use `jansu-broker` +
  `jansu-client` as before.
