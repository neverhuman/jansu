#!/usr/bin/env bash
# CI doctor — verifies local environment matches CI requirements
set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel)"
source "$REPO_ROOT/ops/ci/lib.sh"

echo "==> checking rust toolchain"
rustc --version
cargo --version

echo "==> checking just"
just --version

echo "==> checking docker"
docker --version

echo "==> fmt check"
ci_fmt

echo "==> clippy check"
ci_clippy

echo "==> CI doctor passed"
