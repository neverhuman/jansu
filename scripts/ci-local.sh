#!/usr/bin/env bash
# Local CI runner — mirrors GitHub Actions pipeline steps locally
set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel)"
source "$REPO_ROOT/ops/ci/lib.sh"

echo "==> fmt"
ci_fmt

echo "==> clippy"
ci_clippy

echo "==> check"
ci_check

echo "==> test"
ci_test

echo "==> doc"
ci_doc

echo "==> security"
ci_security

echo "==> local CI passed"
