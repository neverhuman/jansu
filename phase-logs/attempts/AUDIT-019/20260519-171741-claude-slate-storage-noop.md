# Agent

Claude Opus 4.7 (1M context)

# Prompt

Address `jansu-storage/src/slate/storage.rs` (2609 LOC) for HLT-001 shape pressure (Task #18, Step A5 of plan v2).

# Phase Or Audit Item

AUDIT-019 cross-phase Jankurai score recovery (Step A5).

# Files Read

- jansu-storage/src/slate/storage.rs (head + grep scan)

# Files Changed

- None. This batch is intentionally a NO-OP, documented for honesty.

# Tests Added

- None.

# Verification Commands

- `grep -nE "^impl |^pub fn|^fn |^pub trait|^#\[cfg\(test\)\]|^mod tests" jansu-storage/src/slate/storage.rs` → single `impl Storage for Engine` at line 72; no inline tests module.
- `grep -nE "unwrap_or_else\(Vec::new\)|unwrap_or_else\(String::new\)|todo!\(\)|unimplemented!\(\)" jansu-storage/src/slate/storage.rs` → no matches.

# Why no-op

slate/storage.rs is structurally identical to lite/storage.rs (A4):
- A single `impl Storage for Engine { ... }` block spans the entire file. Rust does not allow splitting a trait implementation across multiple source files.
- The file contains ZERO hidden `todo!()`, `unimplemented!()`, or `unwrap_or_else(Vec::new|String::new)` patterns that a per-file scan threshold change would expose. (Unlike pg.rs in A1, which had two `todo!()` and one `unwrap_or_else`, and unlike lite/storage.rs in A4, which had 4 `unwrap_or_else` patterns.)

There is nothing to pre-emptively clean and no syntactically valid way to split the file with the constraints of:
- Rust trait-impl single-file requirement
- Phase 08 product-code regression safety (the user's "no regression" constraint)
- The honest-audit principle (no detector-bait, no masking)

# Outcome

No changes. The file remains at 2609 LOC. It contributes to the workspace `max_loc > 1000` penalty along with lib.rs (3211), lite/storage.rs (2925), pg/storage_dispatch.rs (2680), and dynostore/storage.rs (2624). HLT-001 stays at score 35 for the same structural reason all sessions have hit: the workspace has many large authored files and a deep refactor (every Storage trait method extracted to a free function) would be needed to drop all of them under 1000 LOC.

# Residual Risks

- slate/storage.rs is now the smallest of the five 2000+ LOC `impl Storage for X` files in the workspace, but it still triggers the >1000 LOC penalty. Future work that wants to actually move HLT-001 must restructure Storage so that backend impls are thin dispatchers calling helper modules.

# Next Recommended Action

- Proceed to Task #19: final verification + AUDIT.md honest close-out. The work this session is done; HLT-001 cannot be closed in this session without dishonest workarounds.
