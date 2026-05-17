# Attempt log

## Agent

Cursor (GPT-5.2)

## Prompt

Study tips and MASTER_PLAN; next logical phase; update logs; keep going until work is hardened and verified.

## Phase Or Audit Item

Phase **08** — `AUDIT-004` (Fetch ReadCommitted non-txn proof)

## Files Read

- `MASTER_PLAN.md`, `AUDIT.md`, `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- `jansu-storage/tests/fetch.rs`

## Files Changed

- `jansu-storage/tests/fetch.rs`
- `docs/compatibility/kafka-4.2-ledger.json`
- `AUDIT.md`
- `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- `phase-logs/08-fetch-list-offsets-leader-epoch.md.log` (supplement present)
- `phase-logs/attempts/08-fetch-list-offsets-leader-epoch/20260505-fetch-read-committed-non-txn-cursor.md`

## Tests Added

- `jansu-storage/tests/fetch.rs::phase08::fetch_read_committed_matches_uncommitted_when_no_transactions`

## Verification Commands

- `cargo test -p jansu-storage --test fetch --no-default-features --features dynostore -- --nocapture`
- `just compatibility-contract`
- `cargo fmt --all --check`

## Outcome

- Always-on storage proof that ReadCommitted Fetch matches ReadUncommitted when LSO==HWM; ledger API key **1** updated.

## Residual Risks

- No broker-matrix duplicate; transactional read_committed and ListOffsets read_committed still open.

## Next Recommended Action

- Broker `simple_non_txn` extension or slate/redlinedb parity test for same assertion if desired; else transactional abort coverage with Phase 12.
