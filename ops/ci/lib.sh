#!/usr/bin/env bash
# CI helper library — sourced by scripts/ci-local.sh and scripts/ci-doctor.sh
set -euo pipefail

ci_check()   { cargo check --workspace --all-features --all-targets; }
ci_fmt()     { cargo fmt --all --check; }
ci_clippy()  { cargo clippy --workspace --all-features --all-targets -- -D warnings; }
ci_test()    { cargo nextest run --workspace --all-features --no-fail-fast; }
ci_doc()     { cargo test --workspace --doc --all-features; }
ci_security(){ bash tools/security-lane.sh; }
