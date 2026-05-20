# Agent

Claude Opus 4.7 (1M context)

# Prompt

Clear HLT-016 (supply-chain), HLT-018 (build speed), HLT-004 (Cargo.lock unmapped) from the Jankurai 1.5.0 gate findings.

# Phase Or Audit Item

AUDIT-019 cross-phase Jankurai score recovery — manifest + workspace metadata only.

# Files Read

- .gitignore
- agent/test-map.json
- justfile
- agent/owner-map.json (for .github/ pattern reference)
- docs/testing.md
- Cargo.toml (profile section)

# Files Changed

- `.gitignore` — removed `Cargo.lock` line so the detector credits "locked dependency graph present" on the Build speed and Security dimensions. Cargo.lock is already tracked; this only changes the documented intent.
- `agent/test-map.json` — added `Cargo.lock` route mapping to `just check` (mirrors the existing `Cargo.toml` route).
- `justfile` — added three new lanes for HLT-018 intent alignment:
  - `test-crate <crate>` — narrow per-crate nextest lane
  - `test-changed <ref>` — changed-since lane for incremental verification
  - `check-incremental` — keep-going workspace check

# Tests Added

- None (manifest + tooling changes only).

# Verification Commands

- `just --list` → all three new recipes parse and show in help
- Cache-clearing pattern for deterministic results: `rm -f target/jankurai/audit-state.json && just score`
  - Smart mode caches `last_full_scan_commit` in `target/jankurai/audit-state.json` and switches to `changed-fast` on subsequent runs, which produces inconsistent dimensional scores when files change. CI sees a fresh checkout so this only affects local iteration.
- `just score` (after cache clear) → `score=82 raw=82 caps=0/0 new_caps=0 findings=2/0 new_findings=2`

# Outcome

Score improved from `80 caps=0 findings=3` to `82 caps=0 findings=2`:

| Dimension | Was | Now |
|---|---|---|
| Security and supply-chain posture | 74 | 86 (HLT-016 cleared) |
| Build speed signals | 70 | 80 (HLT-018 still below floor 85) |
| Code shape and semantic surface | 35 | 35 (HLT-001 still leads — pending split) |

HLT-016 cleared by Cargo.lock recognition. HLT-004 cleared by test-map route. HLT-018 partially improved — dimension reached 80 but the 85 floor still trips the finding. Attempted but reverted: `.cargo/config.toml` (no dimension impact) and `.config/nextest.toml` (no dimension impact). The new justfile lanes provide real iteration value even though the detector didn't credit them with a sixth marker.

# Residual Risks

- HLT-018 dimension stays at 80 (5 below floor). The detector's scoring formula is opaque from outside; further experimentation would be guess-and-check. The new justfile lanes are aligned with the rule's intent ("fast deterministic build/test targets, caches, and narrow proof lanes for agent iteration") and are useful in their own right.
- Removing `Cargo.lock` from `.gitignore` makes the lockfile commit-required, which is correct for an application/CLI workspace (Jansu ships a single binary) but is unusual for a library workspace. The file was already tracked so behavior is unchanged.

# Next Recommended Action

- Tackle HLT-001 (split `jansu-storage/src/lib.rs` 4576 LOC). That fix should also dent the workspace bad-behavior signal count and may help Build speed dimension cross 85 indirectly.
