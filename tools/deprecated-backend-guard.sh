#!/usr/bin/env bash
set -euo pipefail

# Deprecated backend guard.
# Fails CI if removed storage backends (postgres / sqlite / libsql / turso / limbo)
# reappear in jansu product Rust source or Cargo manifests.
#
# Scope:
#   - jansu-*/src/**/*.rs           (product source)
#   - jansu-*/Cargo.toml            (crate manifests)
#   - Cargo.toml (workspace root)
#
# Allowed (NOT flagged):
#   - docs/                         (deprecation docs may mention)
#   - agent/                        (boundaries.toml stack-id labels)
#   - jansu-*/tests/                (historical Kafka compat ledger refs)
#   - jansu-*/benches/
#   - docs/compatibility/           (kafka-4.2-ledger.json keeps historical data)
#   - target/, .git/, .claude/, .jankurai/
#   - README.md, justfile, this script itself

REPO_ROOT="${REPO_ROOT:-$(git rev-parse --show-toplevel)}"
cd "$REPO_ROOT"

# Patterns to flag in product code (Rust source + Cargo manifests).
# Each pattern is a forbidden literal substring.
FORBIDDEN=(
  "tokio_postgres"
  "deadpool_postgres"
  "tokio-postgres"
  "deadpool-postgres"
  'feature = "postgres"'
  "feature = \"libsql\""
  "feature = \"turso\""
  "feature = \"limbo\""
  "mod pg;"
  "pub mod pg;"
)

EXIT_CODE=0
HITS=0

scan() {
  local pat="$1"
  local matches
  matches=$(grep -rn --include='*.rs' --include='Cargo.toml' "$pat" jansu-* Cargo.toml 2>/dev/null \
    | grep -v "/tests/" \
    | grep -v "/benches/" \
    | grep -v "/target/" \
    || true)
  if [[ -n "$matches" ]]; then
    echo "FORBIDDEN: $pat" >&2
    echo "$matches" >&2
    echo "" >&2
    HITS=$((HITS + 1))
    EXIT_CODE=1
  fi
}

for pat in "${FORBIDDEN[@]}"; do
  scan "$pat"
done

if [[ $EXIT_CODE -ne 0 ]]; then
  echo "Deprecated backend guard FAILED: $HITS forbidden pattern(s) reappeared." >&2
  echo "See docs/storage/redlinedb-feedback.md (action item 4)." >&2
  echo "Postgres/sqlite/libsql/turso/limbo backends were removed on 2026-05-17 (commit 1373402)." >&2
  exit 1
fi

echo "Deprecated backend guard OK: no forbidden patterns in product code."
