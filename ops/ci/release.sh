#!/usr/bin/env bash
# Release readiness lane for local and CI release checks.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
source "$REPO_ROOT/ops/ci/lib.sh"

ci_release_check
just compatibility-contract
just security
