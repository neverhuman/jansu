# Agent

Claude Opus 4.7 (1M context)

# Prompt

Split `jansu-schema/src/avro/arrow.rs` (3746 LOC) for HLT-001 shape pressure.

# Phase Or Audit Item

AUDIT-019 cross-phase Jankurai score recovery (Step A3 of plan v2).

# Files Read

- jansu-schema/src/avro/arrow.rs (full)
- /home/ubuntu/jankurai/crates/jankurai/src/audit/scan.rs::is_test_or_example_path (filter list)
- /home/ubuntu/jankurai/crates/jankurai/src/audit/helpers.rs::product_code_files

# Files Changed

- `jansu-schema/src/avro/arrow.rs`: 3746 → 1374 LOC. Removed the inline `#[cfg(test)] mod tests { ... }` block (2371 LOC of test bodies) and replaced it with `#[cfg(test)] mod tests;`.
- `jansu-schema/src/avro/arrow/tests.rs`: NEW, 2390 LOC (test body with proper de-indent + Apache header). The path ends in `/tests.rs`, which the auditor's `is_test_or_example_path` filter excludes from product-code surface counts.

The choice to extract tests (rather than the impl) was deliberate: the production code in arrow.rs (1374 LOC after the move) is a small/medium graph of helper fns and one impl trait per receiver — splitting it further would require ~5 fragment files for ~200 LOC saved each, with low scoring payoff. The tests block is 2371 LOC of pure test scaffolding and is the obvious large lump.

# Tests Added

- None — pure mechanical move. The original `mod tests { ... }` body is preserved byte-for-byte (modulo a 4-space dedent because the body is no longer nested inside a `mod tests` block).

# Verification Commands

- `cargo check -p jansu-schema --all-features` → ok (126 crates compiled, 1m 1s; 0 errors, 0 warnings)
- `cargo nextest run -p jansu-schema --all-features --lib -E 'test(/avro/)'` → **28/28 avro tests pass** (25 `avro::arrow::tests::*` from the moved module, all green — proves the test module file is correctly wired)
- `cargo nextest run -p jansu-schema --all-features --lib --no-fail-fast` → 91 passed, 4 failed, 12 skipped. The 4 failures are `lake::berg::tests::*` (iceberg-infra-dependent) and are **PRE-EXISTING**, confirmed via `git stash && cargo nextest` vs `git stash pop`.
- `cargo fmt --all --check` → ok
- `rm -f target/jankurai/audit-state.json && just score` → `score=82 raw=82 caps=0/0 new_caps=0 findings=2/0 new_findings=2`

# Outcome

arrow.rs cut from 3746 → 1374 LOC (63% reduction). 2371 LOC of tests now live in `avro/arrow/tests.rs` and are excluded from product-code surface counts (per `is_test_or_example_path` filter at jankurai `scan.rs:131`).

Observable Jankurai dimension shifts:
- "largest authored code file" evidence flipped from `jansu-schema/src/avro/arrow.rs (3746 LOC)` to **`jansu-storage/src/lib.rs (3211 LOC)`** (lib.rs is now the workspace leader).
- "rust bad-behavior advisory signals" dropped from 1516 to 1489 (27 fewer signals — the extracted tests no longer contribute to the count).
- "copy-code advisory classes" dropped from 277 to 276 (one fewer duplicate-class detection).

The shape dimension score stays at 35 because `max_loc` is still over 1000 LOC (lib.rs 3211, lite/storage.rs 2913, pg/storage_dispatch.rs 2680, dynostore/storage.rs 2624, slate/storage.rs 2609 all remain above the threshold).

Same gate verdict (`score=82 caps=0 findings=2`), but the underlying surface is meaningfully smaller.

# Residual Risks

- The 4 `lake::berg::tests` failures are pre-existing and require an iceberg catalog (lakekeeper) running — outside the AUDIT-019 scope. They were failing before A3 and are still failing; A3 did not cause them.
- Unlike pg.rs (A1), arrow.rs did NOT have hidden `todo!()` placeholders, so no extra cleanup work was needed.

# Next Recommended Action

- Proceed to Task #17: split `jansu-storage/src/lite/storage.rs` (2913 LOC). Note: lite/storage.rs contains 4 `unwrap_or_else(Vec::new|String::new)` patterns (`grep`-confirmed) that the size-based scan threshold currently hides — splitting will expose them, mirroring the A1 pg.rs experience. Plan to convert them to explicit `match` blocks during the same batch.
