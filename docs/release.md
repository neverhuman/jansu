# Release

The workspace version in `Cargo.toml` is the release version source. Release
tags must use the same version with a `v` prefix, for example `v0.6.2`.

## Required Evidence

- `just release-check` builds the release binary and writes a checksum under
  `target/release/`.
- `just test` covers the full workspace test lane and is the baseline sanity
  check before publication.
- `just security` writes security evidence under `target/jankurai/security/`
  including `evidence.json`, `actionlint.log`, `gitleaks.json`, `cargo-audit.json`,
  and `sbom.json`.
- `just score` writes `agent/repo-score.json` and `agent/repo-score.md`, then
  mirrors them into `target/jankurai/repo-score.json` and
  `target/jankurai/repo-score.md` during the local CI lane.
- `just compatibility-contract` verifies that advertised compatibility claims
  are backed by `docs/compatibility/kafka-4.2-ledger.json`.

## Publishing Rules

- Publish only from a clean `main` checkout.
- Do not skip the security, audit, compatibility, or checksum lanes.
- GitHub Actions must use SHA-pinned actions and explicit permissions.
- Keep release artifacts tied to the tag that built them.

## Rollback

Before publication, delete the local and remote tag and rebuild from the fixed
commit. After publication, prefer a follow-up patch release over rewriting
history.

## Launch gate

The release gate is artifact-backed.  No release tag may be published unless
every item below has a recorded receipt under `target/release/` or
`target/jankurai/`.  The gate fails the build if any item is missing.

- **Security**: `just security` writes `target/jankurai/security/evidence.json`
  with gitleaks, cargo-audit, actionlint, and SBOM (syft) results.  Hard failure
  exits the pipeline.
- **Backup**: storage backends are stateless.  The PostgreSQL backend backup
  policy lives in the operator runbook at `docs/ops/backups.md`; the S3-backed
  object backend relies on the upstream bucket lifecycle/versioning policy
  referenced there.  Restore drills are recorded per release in
  `phase-logs/attempts/cross-phase/`.
- **Monitoring**: every release ships with `jansu-otel` telemetry helpers wired
  to the Prometheus/Grafana compose stack documented in `docs/observability.md`.
  Release runbook lists the required alerts (broker liveness, fetch latency,
  consumer-group lag, retention/compaction backlog).
- **Rollback**: the rollback playbook above plus the rollback procedure in
  `docs/ops/rollback.md` (tag deletion, binary rebuild, ledger revert, client
  reroute).  Post-publication, follow-up patch releases are preferred over
  history rewrites.
- **Abuse controls / rate limits**: `jansu-auth` enforces SCRAM/SASL on every
  request; ACL revocation is via the AdminClient `DeleteAcls` path tested by
  `jansu-broker/tests/auth.rs`.  Quotas and rate-limit budgets are documented in
  `docs/security/authz-matrix.md` and `docs/ops/cost-budget.md`.
