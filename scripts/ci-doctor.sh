#!/usr/bin/env bash
# Verify local prerequisites for the same lanes used by CI.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
source "$REPO_ROOT/ops/ci/lib.sh"

echo "rustc: $(rustc --version)"
echo "cargo: $(cargo --version)"
echo "just: $(just --version)"
echo "docker: $(docker --version 2>/dev/null || echo unavailable)"
echo "jankurai: $(jankurai --version 2>/dev/null || echo unavailable)"

ci_fast
echo "ci-doctor: ok"
