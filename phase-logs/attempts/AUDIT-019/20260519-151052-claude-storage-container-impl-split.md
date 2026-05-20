# Agent

Claude Opus 4.7 (1M context)

# Prompt

Split `jansu-storage/src/lib.rs` (4576 LOC) to reduce HLT-001-DEAD-MARKER shape pressure.

# Phase Or Audit Item

AUDIT-019 cross-phase Jankurai score recovery. Structural cleanup only — no behavior change.

# Files Read

- jansu-storage/src/lib.rs (full, in chunks)
- Existing split pattern in jansu-storage/src/lite/ and jansu-storage/src/limbo/
- agent/repo-score.json dimension evidence

# Files Changed

- `jansu-storage/src/lib.rs` — 4576 → 3194 LOC. Removed lines 3191-4555 (the `impl Storage for StorageContainer` block). Promoted two static metric counters (`STORAGE_CONTAINER_REQUESTS`, `STORAGE_CONTAINER_ERRORS`) from file-private to `pub(crate)` so the extracted impl can reach them. Added `mod container_impl;`.
- `jansu-storage/src/container_impl.rs` — NEW, 1383 LOC. Holds the dispatcher impl block verbatim, plus a one-line `use super::*;` to inherit lib.rs's namespace. Public API surface of the crate is unchanged.

# Tests Added

- None. The extracted code is byte-identical except for the wrapping module preamble and the `use super::*` import.

# Verification Commands

- `cargo check -p jansu-storage --all-features` → ok (3m 12s, 1 crate)
- `cargo check --workspace --all-features --all-targets` → ok (2m 33s, 17 crates)
- `cargo nextest run -p jansu-storage --lib --all-features` → `64 tests run: 64 passed, 6 skipped`
- `cargo fmt --all --check` → ok (after removing a blank line left by `sed`)
- `just fast` → ok
- `rm -f target/jankurai/audit-state.json && just score` → `score=82 raw=82 caps=0/0 new_caps=0 findings=2/0 new_findings=2` (unchanged from pre-split: 2 findings remain)

# Outcome

`jansu-storage/src/lib.rs` shrunk by 1382 LOC. The "largest authored code file" evidence flipped from `jansu-storage/src/lib.rs (4576 LOC)` to `jansu-storage/src/pg.rs (4045 LOC)`. The HLT-001 shape dimension score did NOT move (stayed at 35).

The Jankurai shape dimension formula is dominated by:
1. `rust bad-behavior advisory signals: 1513` — workspace-wide pattern count, unaffected by file moves
2. `code file exceeds 1000 LOC` — triggers as long as ANY authored file is over 1000 lines (pg.rs at 4045, dynostore.rs at 3580, avro/arrow.rs at 3746, and others remain over)
3. `copy-code advisory classes found: 277` — advisory only per the evidence note

Reducing the dimension below 85 requires either a workspace-wide cleanup of rust bad-behavior patterns or splitting every authored file over 1000 LOC — both far outside this session's scope.

The split is still worth keeping: it improves crate modularity, build-time parallelism, and IDE navigation, and matches the established lite/limbo module-split pattern. It just doesn't move the gate.

# Residual Risks

- HLT-001 (Code shape and semantic surface, score 35) remains a finding. Splitting pg.rs, dynostore.rs, and avro/arrow.rs would be the next moves but each is a multi-hour task with its own clippy/test risk.
- HLT-018 (Build speed signals, score 80) remains a finding. The dimension's 5-point gap to the 85 floor is opaque from outside; further experimentation with `.cargo/config.toml` and `.config/nextest.toml` did not move it (both reverted).
- Pre-existing clippy warnings: `cargo clippy --workspace --all-features --all-targets -- -D warnings` fails on 30 warnings (19 `manual_unwrap_or_default`, 9 `unwrap_or_else` default, 4 if-identical-blocks, etc.) in files I did NOT touch (jansu-schema/src/proto.rs, jansu-storage/src/dynostore.rs, jansu-storage/src/pg.rs, etc.). This is a pre-existing condition outside AUDIT-019 scope but should be tracked separately.

# Next Recommended Action

- Move to final verification (close AUDIT-019 with residuals documented). Closing HLT-001 and HLT-018 needs a deliberate multi-session shape pass: split each crate's largest authored file, then address the rust bad-behavior pattern count workspace-wide.
