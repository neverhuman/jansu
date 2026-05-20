#!/usr/bin/env bash
# Shared local/CI helpers. Keep workflow logic here so local runs and GitHub
# Actions call the same commands.
set -euo pipefail

CARGO_NEXTEST_VERSION="${CARGO_NEXTEST_VERSION:-0.9.135}"
CARGO_AUDIT_VERSION="${CARGO_AUDIT_VERSION:-0.22.1}"
GITLEAKS_VERSION="${GITLEAKS_VERSION:-8.28.0}"
SYFT_VERSION="${SYFT_VERSION:-1.25.0}"

ci_repo_root() {
    git rev-parse --show-toplevel
}

ci_check() {
    cargo check --workspace --all-features --all-targets
}

ci_fmt() {
    cargo fmt --all --check
}

ci_clippy() {
    cargo clippy --workspace --all-features --all-targets -- -D warnings
}

ci_test() {
    cargo nextest run --workspace --all-targets --all-features --no-fail-fast --exclude fuzz
    cargo test --workspace --doc --all-features
}

ci_security() {
    bash "$(ci_repo_root)/tools/security-lane.sh"
}

ci_compatibility_contract() {
    cargo test -p jansu-broker --test compatibility_contract --all-features -- --nocapture
}

ci_fast() {
    ci_fmt
    bash -n scripts/ci-local.sh
    bash -n tools/checks/jankurai-gate.sh
    bash -n tools/checks/no-mask.sh
    bash -n tools/security-lane.sh
    bash tools/checks/no-mask.sh
    if command -v actionlint >/dev/null 2>&1; then
        actionlint .github/workflows/*.yml
    else
        echo "actionlint unavailable; syntax-only shell fast lane completed" >&2
    fi
}

ci_release_check() {
    cargo build --release --bin jansu --no-default-features --features delta,dynostore,iceberg,libsql,parquet,postgres,slatedb
    mkdir -p target/release
    sha256sum target/release/jansu > target/release/jansu.sha256
}
