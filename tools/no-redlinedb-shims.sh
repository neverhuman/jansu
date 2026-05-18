#!/usr/bin/env bash
set -euo pipefail

PATTERNS=(
    "RedlineTimestamp"
    "trait IntoRedlineValue"
    "trait FromRedlineValue"
    "trait ValueExt"
    "trait IntoParams"
    "pub(crate) struct Pool"
    "redline_error("
)

TARGET="jansu-storage/src/redlinedb"
FAIL=0

for pat in "${PATTERNS[@]}"; do
    if grep -rn "$pat" "$TARGET" --include="*.rs" 2>/dev/null; then
        echo "FAIL: banned pattern found: $pat"
        FAIL=1
    fi
done

[ "$FAIL" -eq 0 ] && echo "OK: no banned redlinedb shim patterns" || exit 1
