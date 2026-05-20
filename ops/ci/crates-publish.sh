#!/usr/bin/env bash
# crates.io publish lane. The workflow provides CARGO_REGISTRY_TOKEN through
# rust-lang/crates-io-auth-action.
set -euo pipefail

: "${CARGO_REGISTRY_TOKEN:?CARGO_REGISTRY_TOKEN is required}"

mkdir -p target/jankurai/release
cargo metadata --format-version 1 > target/jankurai/release/cargo-metadata.json
sha256sum Cargo.lock > target/jankurai/release/Cargo.lock.sha256
if command -v syft >/dev/null 2>&1; then
    syft . -o json > target/jankurai/release/sbom.json
else
    printf '{"status":"missing","tool":"syft"}\n' > target/jankurai/release/sbom.json
fi

cargo publish --workspace
