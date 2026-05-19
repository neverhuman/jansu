#!/usr/bin/env bash
# Main CI test lane called by .github/workflows/ci.yml.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

sudo apt-get update
sudo apt-get install -y libclang-dev libkrb5-dev
cargo install cargo-nextest --version "${CARGO_NEXTEST_VERSION:-0.9.135}" --locked
just ci
just test
