# 2026-05-18 - Phase 08 Fetch Control Batch Visibility

## Scope

Fix Fetch parity so control batches never surface as user-visible records, and keep aborted transactional visibility correct under `ReadCommitted`.

## Files

- jansu-storage/src/dynostore.rs
- jansu-storage/src/service/fetch.rs
- jansu-storage/tests/fetch.rs
- phase-logs/08-fetch-list-offsets-leader-epoch.md.log
- AUDIT.md

## Verification

- `cargo fmt --all --check`
- `git diff --check`
- `env CARGO_TARGET_DIR=/tmp/jansu-verify-read-committed cargo test -p jansu-storage --test fetch --no-default-features --features dynostore --jobs 20 -- --nocapture`
- `env CARGO_TARGET_DIR=/tmp/jansu-verify-contract-read-committed cargo test -p jansu-broker --test compatibility_contract --all-features --jobs 20 -- --nocapture`

## Outcome

- Fetch now drops control batches for all isolation levels and hides aborted transactional batches under `ReadCommitted`.
- The read-committed regression now proves the visible record is the later non-transactional batch at offset `2`.
- The broker compatibility contract still passes after the storage-side fetch fix.
