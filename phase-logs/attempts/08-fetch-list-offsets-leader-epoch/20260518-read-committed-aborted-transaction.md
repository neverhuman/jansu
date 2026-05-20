# 2026-05-18 - Phase 08 Read-Committed Aborted Transaction Visibility

## Scope

Harden Fetch `ReadCommitted` handling so aborted transactional batches are hidden, the visible-byte budget is rechecked after filtering, and aborted transaction metadata survives transaction finalization in DynoStore.

## Files

- jansu-storage/src/dynostore.rs
- jansu-storage/src/service.rs
- jansu-storage/src/service/fetch.rs
- jansu-storage/tests/fetch.rs
- AUDIT.md
- docs/compatibility/kafka-4.2-ledger.json
- phase-logs/08-fetch-list-offsets-leader-epoch.md.log
- phase-logs/index.json

## Verification

- `cargo fmt --all --check`
- `git diff --check`
- `env CARGO_TARGET_DIR=/tmp/jansu-verify-fetch-read-committed cargo test -p jansu-storage --test fetch --no-default-features --features dynostore --jobs 20 -- --nocapture`
- `env CARGO_TARGET_DIR=/tmp/jansu-verify-contract-read-committed cargo test -p jansu-broker --test compatibility_contract --all-features --jobs 20 -- --nocapture`

## Outcome

- `ReadCommitted` fetch now hides aborted transactional records and still reports the aborted transaction metadata.
- DynoStore persists aborted transaction ranges by topic-partition after `txn_end(false)`.
- Request-channel storage routing now forwards aborted transaction range queries.
