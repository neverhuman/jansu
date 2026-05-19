#!/usr/bin/env bash
# Refuse to run if anyone has masked authored Rust product code from the
# Jankurai audit. Masking inflates the score artificially and was the root
# cause of the AUDIT-019 trust-the-number-versus-trust-the-code regression
# in commit 8a303a4 (2026-05-16).
#
# Allowed: nothing in agent/audit-policy.toml [scan].excluded_paths may end
#          in .rs.
# Allowed: nothing in agent/generated-zones.toml may list a .rs path other
#          than build scripts (named `build.rs`) -- build scripts are
#          authored but emit other files; their inclusion in generated-zones
#          is about the OUTPUT, not the script itself.
#
# The Jankurai gate (tools/checks/jankurai-gate.sh) and the local fast lane
# (ops/ci/lib.sh::ci_fast) both invoke this script so masking attempts fail
# fast at local check time and in CI. Bypassing the rule requires editing
# this script, which is reviewable.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

failed=0

POLICY="agent/audit-policy.toml"
if [ -f "$POLICY" ]; then
    masked_rs="$(awk '/^\[scan\]/{p=1; next} p && /^\[/{p=0} p' "$POLICY" \
        | grep -oE '"[^"]+\.rs"' || true)"
    if [ -n "$masked_rs" ]; then
        echo "JANKURAI GATE FAILED - Rust product code masked in $POLICY [scan].excluded_paths:" >&2
        while IFS= read -r entry; do
            [ -n "$entry" ] && printf '  %s\n' "$entry" >&2
        done <<< "$masked_rs"
        echo "" >&2
        echo "Masking authored .rs files inflates the score artificially. See AUDIT-019" >&2
        echo "history (commit 8a303a4) for the regression this rule prevents." >&2
        echo "Fix: remove the .rs entries from $POLICY and clear the underlying findings honestly." >&2
        failed=1
    fi
fi

ZONES="agent/generated-zones.toml"
if [ -f "$ZONES" ]; then
    masked_zones="$(grep -oE 'path = "[^"]+\.rs"' "$ZONES" \
        | grep -v '/build\.rs"' \
        | grep -v '= "build\.rs"' || true)"
    if [ -n "$masked_zones" ]; then
        echo "JANKURAI GATE FAILED - Rust source declared as generated in $ZONES:" >&2
        while IFS= read -r entry; do
            [ -n "$entry" ] && printf '  %s\n' "$entry" >&2
        done <<< "$masked_zones"
        echo "" >&2
        echo "Marking authored .rs files as 'generated' hides them from the audit." >&2
        echo "Build scripts (build.rs) are the only allowed .rs exception." >&2
        failed=1
    fi
fi

exit "$failed"
