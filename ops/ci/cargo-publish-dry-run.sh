#!/usr/bin/env bash
# Crate packaging dry run with release integrity evidence.
set -euo pipefail

mkdir -p target/jankurai/release
cargo metadata --format-version 1 > target/jankurai/release/cargo-metadata.json
sha256sum Cargo.lock > target/jankurai/release/Cargo.lock.sha256
if command -v syft >/dev/null 2>&1; then
    syft . -o json > target/jankurai/release/sbom.json
else
    printf '{"status":"missing","tool":"syft"}\n' > target/jankurai/release/sbom.json
fi
cargo publish --dry-run --workspace
