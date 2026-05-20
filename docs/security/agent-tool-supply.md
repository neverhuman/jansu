# Agent and Tool Supply Chain

This document is the review surface for external tools and managed automation
that can affect builds, tests, audit results, or releases.

## Rust and Cargo

- Rust toolchain: repository toolchain files plus GitHub workflow setup.
- `cargo-nextest`: installed with `--locked`; CI pins the install command in
  `ops/ci/lib.sh`.
- `cargo-audit`: security advisory scan, version pinned in `ops/ci/lib.sh`.

## GitHub Actions

Workflow `uses:` entries must be pinned to full commit SHAs. The current pins
used by active workflows are:

- `actions/checkout`: `de0fac2e4500dabe0009e67214ff5f5447ce83dd`
- `actions/upload-artifact`: `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a`
- `actions/download-artifact`: `3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c`
- `actions-rust-lang/setup-rust-toolchain`: `46268bd060767258de96ed93c1251119784f2ab6`
- `dtolnay/rust-toolchain`: `29eef336d9b2848a0b548edc03f92a220660cdb8`
- `extractions/setup-just`: `f8a3cce218d9f83db3a2ecd90e41ac3de6cdfd9b`
- `crate-ci/typos`: `a8d8e187146634c459c27ade2d3e338569378720`
- `docker/metadata-action`: `030e881283bb7a6894de51c315a6bfe6a94e05cf`
- `docker/setup-buildx-action`: `4d04d5d9486b7bd6fa91e7baf45bbb4f8b9deedd`
- `docker/login-action`: `4907a6ddec9925e35a0a9e82d7399ccc52663121`
- `docker/build-push-action`: `bcafcacb16a39f128d818304e6c9c0c18556b85f`
- `softprops/action-gh-release`: `3bb12739c298aeb8a4eeaf626c5b8d85266b0e65`
- `rust-lang/crates-io-auth-action`: `bbd81622f20ce9e2dd9622e3218b975523e45bbe`
- `github/codeql-action/upload-sarif`: `458d36d7d4f47d0dd16ca424c1d3cda0060f1360`

## Agent Tools

Root instructions live in `AGENTS.md`. Ops-specific instructions live in
`ops/AGENTS.md`. Agents must preserve user changes and route implementation
through phase-backed or audit-backed work.

Review cadence: quarterly, or immediately after a tool security advisory.
