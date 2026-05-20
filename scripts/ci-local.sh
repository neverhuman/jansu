#!/usr/bin/env bash
# Thin local dispatcher for CI-parity commands.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

usage() {
    cat <<'EOF'
Usage:
  scripts/ci-local.sh audit
  scripts/ci-local.sh score
  scripts/ci-local.sh jankurai-gate
  scripts/ci-local.sh fast
  scripts/ci-local.sh check
  scripts/ci-local.sh fmt
  scripts/ci-local.sh clippy
  scripts/ci-local.sh test
  scripts/ci-local.sh security
  scripts/ci-local.sh compatibility-contract
EOF
}

cmd="${1:-}"
case "$cmd" in
    audit | score | jankurai-gate)
        exec bash "$REPO_ROOT/tools/checks/jankurai-gate.sh"
        ;;
    fast | check | fmt | clippy | test | security | compatibility-contract)
        # shellcheck source=/dev/null
        source "$REPO_ROOT/ops/ci/lib.sh"
        case "$cmd" in
            fast) ci_fast ;;
            check) ci_check ;;
            fmt) ci_fmt ;;
            clippy) ci_clippy ;;
            test) ci_test ;;
            security) ci_security ;;
            compatibility-contract) ci_compatibility_contract ;;
        esac
        ;;
    -h | --help | help | "")
        usage
        ;;
    *)
        echo "unknown ci-local target: $cmd" >&2
        usage >&2
        exit 2
        ;;
esac
