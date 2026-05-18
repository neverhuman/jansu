# Agent and Tooling Supply Chain

This document is the pinned-version catalogue for every external tool,
plugin, CLI, action, and managed agent that participates in the Jansu
build, test, audit, release, or developer-experience flow. The audit
lane (`security`) reads this file as the source of truth when it
checks that nothing has slipped in unpinned.

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

For each entry the catalogue records the name, the version currently
pinned (or an explicit statement that the entry is unpinned), the file
that holds the pin, and the upstream source the pin was taken from.

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

- Pinned version: `unpinned (latest)`. The CI workflow at
  `.github/workflows/ci.yml:64` installs `cargo-nextest` with
  `cargo install cargo-nextest --locked`, which selects the latest
  published version that satisfies the dependency lockfile shipped
  with `cargo-nextest` itself.
- Risk: A new `cargo-nextest` release that changes the default test
  filter behaviour, the output format, or the exit-code convention
  could break the test workflow without warning.
- Remediation plan: Move the install to a version-pinned form
  (`cargo install cargo-nextest --version <X.Y.Z> --locked`) and
  record the pinned version here. The remediation item is tracked in
  `AUDIT.md`.

### `cargo-audit`

- Pinned version: `0.22.1`. The Jankurai workflow installs it with
  `cargo install cargo-audit --version 0.22.1 --locked`.
- Where invoked: `tools/security-lane.sh`, exposed locally as
  `just security` and in CI through `.github/workflows/jankurai.yml`.
- Failure mode: the advisory JSON is always written to
  `target/jankurai/security/cargo-audit.json`. The Jankurai lane keeps
  dependency advisories visible without hiding the rest of the audit
  receipt.

### Jankurai security lane CLIs

- `actionlint` - pinned to `v1.7.7` in `.github/workflows/jankurai.yml`
  with `go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.7`.
- `zizmor` - pinned to `1.24.1` in `.github/workflows/jankurai.yml`
  with `cargo install zizmor --version 1.24.1 --locked`.
- `gitleaks` - pinned to `v8.30.1` in `.github/workflows/jankurai.yml`
  with `go install github.com/gitleaks/gitleaks/v8@v8.30.1`.
- `syft` - pinned to `v1.44.0` in `.github/workflows/jankurai.yml`
  with `go install github.com/anchore/syft/cmd/syft@v1.44.0`.

### `cargo-about`

- Pinned version: `unpinned`. The repository carries an `about.toml`
  and an `about.hbs` template under the repository root, which are the
  configuration for `cargo-about` license-report generation. The CLI
  itself is not installed in any in-tree workflow.
- Risk: The license catalogue may drift from the actual dependency
  set if `cargo-about` is not run on every release.
- Remediation plan: Add a pinned `cargo install cargo-about --version
  <X.Y.Z> --locked` step to the release workflow and record the pin
  here.

### `cargo-fuzz`

- Pinned version: `unpinned`. The `justfile` exposes the
  `cargo-fuzz` recipe at line 38 of `justfile`, and the fuzz crate at
  `fuzz/Cargo.toml` depends on `libfuzzer-sys` pinned to `0.4` in the
  workspace `Cargo.toml` at line 111. The `cargo-fuzz` CLI itself
  must be installed locally before any of the fuzz targets can be
  run.
- Risk: Different developers may run different `cargo-fuzz` versions,
  which can change how corpora are written or how crashes are
  bucketed.
- Remediation plan: Pin `cargo-fuzz` in a new `tools.toml` under
  `.config/` and bind the just recipe to that version.

### `cargo` workspace dependency pins (selected, security-bearing)

- `bytes = "1"` - zero-copy byte buffers.
- `chrono = "0.4"` - timestamp arithmetic.
- `crc-fast = "1.8.0"` - record-batch CRC32-C; security-bearing
  because a bug here would let a corrupted batch be accepted as
  valid.
- `libfuzzer-sys = "0.4"` - fuzz runtime; pin lives at
  `Cargo.toml:111`.
- `redlinedb = "=1.0.1"` - RedlineDB backend; security-bearing because a
  bug here would let a crafted SQL statement bypass parameter
  binding.
- `lz4 = "1.28.1"` - record-batch decompression.
- `apache-avro = "0.21.0"` - Avro descriptor parsing.

The complete list lives in `Cargo.toml` under
`[workspace.dependencies]` and is reviewed in lockstep with this
catalogue on every quarterly review.

## 3. GitHub Actions

The active workflows under `.github/workflows/` are `ci.yml`,
`differential-kafka-lab.yml`, `jankurai.yml`, and `release.yml`.
Every third-party action referenced from those files must be
SHA-pinned and listed below with its human-readable version.

- `actions/checkout` - used in `ci.yml`. SHA pin recorded in the
  workflow file; the catalogue records the human-readable tag for
  reviewer convenience.
- `actions-rust-lang/setup-rust-toolchain` - used in `ci.yml`.
  Provides the matching rust toolchain channel from
  `rust-toolchain.toml`. SHA pin recorded in the workflow file.
- `extractions/setup-just` - used in `ci.yml` to install the `just`
  task runner and in `jankurai.yml` to run the security lane. SHA pin
  recorded in the workflow file.
- `actions/upload-artifact` - used in `ci.yml` to persist CI logs.
  SHA pin recorded in the workflow file.
- `dtolnay/rust-toolchain` - used in `jankurai.yml` to install the
  pinned Rust toolchain before Jankurai and security tooling are
  installed. SHA pin recorded in the workflow file.
- `Swatinem/rust-cache` - used in `jankurai.yml` for audit-lane Rust
  build cache reuse. SHA pin recorded in the workflow file.
- `github/codeql-action/upload-sarif` - used in `jankurai.yml` to
  upload Jankurai SARIF. SHA pin recorded in the workflow file.

The audit lane verifies that every `uses:` line in the workflow files
either matches a SHA pin recorded here, or is followed by an inline
human-readable version comment that resolves to a SHA pin recorded
here.

## 4. Container Images

- `grafana/grafana:11.5.1` - dashboard host, declared at
  `compose.yaml:4`. The pin is a concrete tag rather than a SHA; the
  audit lane warns about tag-only pins for runtime services and
  requires a SHA pin for production deployments.
- `quay.io/minio/minio` - S3-compatible object store for the local
  development environment, declared at `compose.yaml:40`. Unpinned in
  the compose file; the audit lane flags this and the remediation is
  to pin to a release tag.
- `jaegertracing/all-in-one` - tracing collector for local
  development, declared at `compose.yaml:78`. Unpinned.
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
