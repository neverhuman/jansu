# Agent

Claude Opus 4.7 (1M context)

# Prompt

Split `jansu-storage/src/dynostore.rs` (3580 LOC) for HLT-001 shape pressure.

# Phase Or Audit Item

AUDIT-019 cross-phase Jankurai score recovery (Step A2 of plan v2).

# Files Read

- jansu-storage/src/dynostore.rs (chunks)
- jansu-storage/src/pg/storage_dispatch.rs (A1 precedent — child module pattern)

# Files Changed

- `jansu-storage/src/dynostore.rs`: 3580 → 977 LOC. Removed lines 654-3256 (the `impl Storage for DynoStore` dispatcher). Added `mod storage;` next to existing `mod metadata;`/`mod opticon;`.
- `jansu-storage/src/dynostore/storage.rs`: NEW, 2624 LOC. Holds the dispatcher impl. Child module of `dynostore` so it retains access to all private helpers on `DynoStore`.

# Tests Added

- None — pure module move.

# Verification Commands

- `cargo check -p jansu-storage --all-features` → ok (1 crate compiled, 12.28s on warm cache, 0 errors, 0 warnings)
- `cargo fmt --all --check` → ok (after `cargo fmt --all` cleared one blank-line drift at dynostore.rs:652)
- `cargo nextest run -p jansu-storage --lib --all-features` → **64 passed, 6 skipped** (Phase 08 fetch_wait, list_offsets, leader_epoch, control_batch all green)
- `rm -f target/jankurai/audit-state.json && just score` → `score=82 raw=82 caps=0/0 new_caps=0 findings=2/0 new_findings=2` — same gate state as before the split

# Outcome

dynostore.rs cut from 3580 → 977 LOC (73% reduction). Unlike pg.rs (A1), the split did NOT expose any hidden `todo!()` placeholders or `unwrap_or_else(Vec::new)` fallback-soup patterns — `grep` confirmed dynostore.rs and dynostore/storage.rs are clean of those markers.

The auditor's `Code shape and semantic surface` dimension remains at score 35 because `max_loc` is still arrow.rs at 3746 LOC (next file in the split queue). The dynostore.rs change drops that file under both the 500 and 1000 LOC thresholds, but until ALL authored files drop under 500 LOC, the dimension penalty stays.

# Residual Risks

- None new. The split follows the established A1 pattern exactly and the dispatcher is byte-identical for the moved methods.
- `cargo clippy` still reports the same 30 pre-existing warnings (none introduced by this split); the closest one is `manual_unwrap_or_default` at `jansu-storage/src/dynostore/storage.rs:814:44` (formerly at `dynostore.rs:814`).

# Next Recommended Action

- Proceed to Task #16: split `jansu-schema/src/avro/arrow.rs` (3746 LOC — now the workspace's largest authored file). Expect to also check it for hidden `todo!()` once it drops below the per-file scan threshold.
