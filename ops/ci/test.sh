#!/usr/bin/env bash
# Main CI test lane — called by .github/workflows/ci.yml
set -euo pipefail

sudo apt-get install -y libclang-dev libkrb5-dev
cargo install cargo-nextest --version 0.9.135 --locked
just ci
just test
