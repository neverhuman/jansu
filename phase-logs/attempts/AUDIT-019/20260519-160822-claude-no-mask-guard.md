# Agent

Claude Opus 4.7 (1M context)

# Prompt

Add a permanent guard so no future agent can mask `.rs` files from the Jankurai audit (the AUDIT-019 8a303a4 regression must not repeat).

# Phase Or Audit Item

AUDIT-019 cross-phase Jankurai score recovery (rule, not finding fix).

# Files Read

- agent/audit-policy.toml (current honest policy)
- agent/generated-zones.toml
- tools/checks/jankurai-gate.sh
- ops/ci/lib.sh
- Historical: `git show 8a303a4:agent/audit-policy.toml` — the regression baseline (12 excluded paths, 8 of them product `.rs` files)

# Files Changed

- `tools/checks/no-mask.sh` (NEW, +63 lines, executable) — fails with a self-documenting error if `agent/audit-policy.toml [scan].excluded_paths` contains any `.rs` entry, or if `agent/generated-zones.toml` lists any `.rs` path other than `build.rs`.
- `tools/checks/jankurai-gate.sh` — invokes `no-mask.sh` right after `require_tool jq`, so EVERY gate run (local `just score`/`just audit`/`just jankurai-gate` and CI `.github/workflows/jankurai.yml`) enforces the rule before `jankurai audit` is ever called.
- `ops/ci/lib.sh::ci_fast` — added a `bash -n` syntax check on `no-mask.sh` and a real invocation, so `just fast` also blocks masking.
- `agent/JANKURAI_STANDARD.md` — new "No Masking Of Authored Rust Code" section explains the rule, the historical regression that caused it, and how the guard is enforced. Points to the script as the bypass-resistance mechanism.

# Tests Added

- The guard itself is a script with no unit tests, but the verification commands below exercise both pass and fail paths end-to-end.

# Verification Commands

- `bash tools/checks/no-mask.sh` (current honest policy) → exit 0 ✓
- Temporarily added `"jansu-storage/src/lib.rs"` to `agent/audit-policy.toml [scan].excluded_paths`, then:
  - `bash tools/checks/no-mask.sh` → exit 1 with the self-documenting error ✓
  - `just score` → fails at the gate with the same error BEFORE `jankurai audit` runs ✓
- Restored the policy → `bash tools/checks/no-mask.sh` exit 0 ✓
- `just fast` → exit 0 (the `bash -n` syntax check and the real `no-mask.sh` call both succeed) ✓
- `just score` (clean) → `score=82 raw=82 caps=0/0 new_caps=0 findings=2/0 new_findings=2` (unchanged baseline)

# Outcome

The guard is in place and verified end-to-end. From this commit onward:

1. Any attempt to add an authored `.rs` file to `audit-policy.toml [scan].excluded_paths` will fail the Jankurai gate locally (`just score`/`just audit`/`just jankurai-gate`), in the local fast lane (`just fast`), and in CI (`.github/workflows/jankurai.yml` invokes the gate).
2. Any attempt to add a non-`build.rs` `.rs` file to `agent/generated-zones.toml` fails the same gates.
3. The error message tells the next agent WHY (with a pointer to the 8a303a4 historical regression) and HOW to fix it honestly (remove the entry, clear the underlying finding through real work or accept a documented residual).
4. Bypassing the rule requires editing `tools/checks/no-mask.sh` itself, which is a reviewable change.

# Residual Risks

- The guard only protects `.rs` files. SQL and JSON files could still be masked if a future regression tries that route. AUDIT-019 history shows no precedent of masking those, but if it becomes an issue, the guard can be extended.
- The guard reads the policy with `awk`/`grep` rather than a TOML parser. If the policy syntax changes (e.g., TOML arrays-of-tables for excluded_paths), the regex may need updating. Acceptable for now since the format has been stable.
- `agent/audit-policy.toml` is still hand-editable; the guard catches violations rather than preventing them at write time.

# Next Recommended Action

- Proceed to Task #14 (split `jansu-storage/src/pg.rs`) as the first HLT-001 file split. The guard is now in place, so subsequent work cannot regress the no-masking principle even by accident.
