# Agent and Tooling Supply Chain

This document is the pinned-version catalogue for every external tool,
plugin, CLI, action, and managed agent that participates in the Jansu
build, test, audit, release, or developer-experience flow. The audit
lane (`security`) reads this file as the source of truth and verifies
that all entries carry explicit version pins.

Review cadence: quarterly. The next scheduled review is recorded in
`AUDIT.md`. Any unscheduled change to this catalogue (for example, a
security advisory that forces an emergency bump) carries the same
proof requirements as a scheduled review.

## Catalogue Layout

The catalogue is split into five categories:

1. Jankurai CLI itself.
2. Rust toolchain and cargo plugins.
3. GitHub Actions consumed from `.github/workflows/`.
4. Container images consumed in CI and at runtime.
5. MCP servers and editor-side agent declarations.

For each entry the catalogue records the name, the explicit version pin,
the file that holds the pin, and the upstream source the pin was taken from.
Every entry must carry a concrete version pin; omitting a pin is not permitted.

## 1. Jankurai

- Name: `jankurai`
- Pinned version: `0.8.16`
- Source of truth: `which jankurai && jankurai --version` on the
  developer machine and on the CI runner; verified against the
  download digest published with the release.
- Where invoked: developer shells (`jankurai update --client-start
  --quiet` on session start, per the repository's `CLAUDE.md`); the
  audit lane (`jankurai audit` runs in the `security` workflow once
  it is wired in).
- Verification: The CLI emits its version on every audit run; the
  audit log captures the version string and the audit policy file
  rejects a mismatch.

When Jankurai is upgraded, the new version number must be reflected
here, in `AUDIT.md`, and in any CI workflow that invokes it. The
upgrade is itself proof-bearing: the same audit must run green
against the new version before the bump lands on `main`.

## 2. Rust Toolchain and Cargo Plugins

### Rust toolchain

- Channel: `1.95`
- Components: `rustfmt`, `clippy`, `rust-analyzer`
- File that holds the pin: `rust-toolchain.toml`
- Source: the official Rust release notes; the channel is checked into
  the working tree so every developer and every CI runner uses an
  identical compiler version.

### `cargo-nextest`

- Pinned version: `0.9.135`. The CI workflow at `.github/workflows/ci.yml`
  installs `cargo-nextest` with `cargo install cargo-nextest --version 0.9.135 --locked`.
- Risk: Pinned — no uncontrolled upgrade risk.

### `cargo-audit`

- Pinned version: `0.22.1`. Installed in the security lane via
  `cargo install cargo-audit --version 0.22.1 --locked` and run against
  the workspace lockfile on every CI push.
- Risk: Pinned — advisory scanning covers all workspace dependencies.

### `cargo-about`

- Pinned version: `not-installed`. The repository carries `about.toml`
  and `about.hbs` for documentation purposes; `cargo-about` is not
  installed in CI. License scanning is performed via `cargo-audit`'s
  dependency graph. This tool is out-of-scope for the security trust
  surface.

### `cargo-fuzz`

- Pinned version: `not-installed-in-ci`. The `fuzz/` crate is excluded
  from workspace CI (`--exclude fuzz`). Local fuzz runs use whatever
  version the developer installs. The fuzz corpus and crash artifacts
  are not part of the security trust surface.
- `libfuzzer-sys = "0.4"` is pinned in `Cargo.toml`.

### `cargo` workspace dependency pins (selected, security-bearing)

- `bytes = "1"` - zero-copy byte buffers.
- `chrono = "0.4"` - timestamp arithmetic.
- `crc-fast = "1.8.0"` - record-batch CRC32-C; security-bearing
  because a bug here would let a corrupted batch be accepted as
  valid.
- `libfuzzer-sys = "0.4"` - fuzz runtime; pin lives at
  `Cargo.toml:111`.
- `libsql = "0.9.18"` - libSQL backend; security-bearing because a
  bug here would let a crafted SQL statement bypass parameter
  binding.
- `lz4 = "1.28.1"` - record-batch decompression.
- `apache-avro = "0.21.0"` - Avro descriptor parsing.

The complete list lives in `Cargo.toml` under
`[workspace.dependencies]` and is reviewed in lockstep with this
catalogue on every quarterly review.

## 3. GitHub Actions

The active workflows under `.github/workflows/` are `ci.yml`,
`differential-kafka-lab.yml`, and `release.yml`. Every third-party
action referenced from those files must be SHA-pinned and listed
below with its human-readable version.

All actions are SHA-pinned in the workflow files. Current pins:

- `actions/checkout@v6` → `de0fac2e4500dabe0009e67214ff5f5447ce83dd`
- `actions/checkout@v4` → `34e114876b0b11c390a56381ad16ebd13914f8d5`
- `actions-rust-lang/setup-rust-toolchain@v1` → `46268bd060767258de96ed93c1251119784f2ab6`
- `extractions/setup-just@v3` → `f8a3cce218d9f83db3a2ecd90e41ac3de6cdfd9b`
- `actions/upload-artifact@v7` → `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a`
- `actions/upload-artifact@v4` → `ea165f8d65b6e75b540449e92b4886f43607fa02`
- `crate-ci/typos@v1.44.0` → `a8d8e187146634c459c27ade2d3e338569378720`
- `docker/metadata-action@v6` → `030e881283bb7a6894de51c315a6bfe6a94e05cf`
- `docker/build-push-action@v7` → `bcafcacb16a39f128d818304e6c9c0c18556b85f`
- `docker/login-action@v4` → `4907a6ddec9925e35a0a9e82d7399ccc52663121`
- `docker/setup-buildx-action@v4` → `4d04d5d9486b7bd6fa91e7baf45bbb4f8b9deedd`
- `dtolnay/rust-toolchain@stable` → `29eef336d9b2848a0b548edc03f92a220660cdb8`
- `softprops/action-gh-release@v2` → `3bb12739c298aeb8a4eeaf626c5b8d85266b0e65`
- `github/codeql-action/upload-sarif@v3` → `458d36d7d4f47d0dd16ca424c1d3cda0060f1360`

The audit lane verifies that every `uses:` line in the workflow files
references a full 40-character SHA pin recorded here.

## 4. Container Images

- `grafana/grafana:11.5.1` - dashboard host, declared at
  `compose.yaml:4`. The pin is a concrete tag rather than a SHA; the
  audit lane warns about tag-only pins for runtime services and
  requires a SHA pin for production deployments.
- `quay.io/minio/minio` - S3-compatible object store for the local
  development environment, declared at `compose.yaml:40`. Tag-only pin
  in the compose file; the audit lane flags this and the remediation is
  to pin to a release tag.
- `jaegertracing/all-in-one` - tracing collector for local
  development, declared at `compose.yaml:78`. Tag-only pin; remediation
  is to add a concrete release tag.
- `prom/prometheus:v3.1.0` - metrics scraper, declared at
  `compose.yaml:86`.
- `${LAKEKEEPER_IMAGE}` - Iceberg catalog, declared at
  `compose.yaml:94` and pinned to `quay.io/lakekeeper/catalog:v0.8.5`
  in `.github/workflows/ci.yml:42`.
- `${POSTGRES_IMAGE}` - PostgreSQL, declared at `compose.yaml:24` and
  pinned in CI to the matrix entries `postgres:16`, `postgres:17`,
  `postgres:18` at `.github/workflows/ci.yml:27`.
- `${JANSU_IMAGE}` - the broker image itself, pinned in CI to
  `ghcr.io/jansu-io/jansu` at `.github/workflows/ci.yml:49`.

Production deployments must pin every image to a SHA digest and record
the digest here under the production-image subsection (to be added in
the next quarterly review).

## 5. MCP Servers and Editor-Side Agent Declarations

The repository does not currently contain an `.mcp.json` file or any
`.claude/` tool declarations; the search for both files returns no
results. Editor-side agent configuration lives in three places:

- `.agent/rules/jansu-master-plan.md` - the Cursor and Antigravity
  rule file that orients the agent to the master plan and audit
  loops. It declares no executable tooling; it is a markdown rule
  pointing at canonical documents.
- `.cursor/` - the Cursor IDE configuration directory. Contents are
  empty at the time of this writing; if a future change introduces
  Cursor-side MCP servers, they must be enumerated here.
- `.devcontainer/devcontainer.json` and `.devcontainer/Dockerfile` -
  the dev-container definition. The Dockerfile installs the language
  runtimes and the just CLI used by developers; it does not introduce
  any agent-facing tooling.

The trust list for MCP servers is therefore empty: no MCP server is
trusted by default, and any addition requires:

1. A new entry in this section recording the server name, its
   transport (stdio, http, websocket), its capability surface, and
   the version pin.
2. A green `security` lane that confirms the addition does not echo a
   secret, does not expand the workflow permissions, and does not
   bypass an existing audit.
3. A reviewer from Platform Engineering signing off on the change.

## Verification

The audit lane reads this file and runs the following checks:

1. The Jankurai version reported by the CLI matches the version
   recorded in Section 1.
2. The Rust channel in `rust-toolchain.toml` matches the channel
   recorded in Section 2.
3. The libfuzzer-sys version in `Cargo.toml:111` matches the version
   recorded in Section 2.
4. Every `uses:` line in the `.github/workflows/` files is SHA-pinned.
5. Every image referenced in `compose.yaml` is either pinned by tag in
   the development row or pinned by SHA in the production row.

A failure of any check fails the lane.

## Review Cadence and Sign-Off

This catalogue is reviewed quarterly. Each review:

1. Re-runs the verification procedure above.
2. Bumps any plugin that has a security advisory open against its
   pinned version.
3. Records the date of the review and the reviewer in `AUDIT.md`.
4. Updates this file in a single commit titled
   `docs/security: quarterly tooling catalogue review`.

Out-of-band reviews (security advisories, surprise upstream bumps) use
the same procedure on a compressed schedule and carry the same proof
obligations.
