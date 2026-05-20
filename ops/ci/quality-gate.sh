#!/usr/bin/env bash
# Fast pre-push quality gate.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
source "$REPO_ROOT/ops/ci/lib.sh"

ci_fast
