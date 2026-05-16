# Release Testing & Checklist

This document provides the **release‑readiness checklist** required by the Jankurai audit. It is used by CI (the `release` workflow) and by developers to verify that a new version can be shipped safely.

---

## 1. Pre‑release steps

- **Version bump** – Update the version in `Cargo.toml` (and any other manifest) and commit the change.
- **Changelog** – Add an entry to `CHANGELOG.md` summarising notable changes, breaking changes, and migration notes.
- **Documentation** – Ensure `README.md` and API docs are up‑to‑date (`just doc`).

## 2. Build artifacts

```sh
just build-dynostore      # Memory-only proof lane for embedded broker work
just release               # Build the release binary
just docker-build          # Build the Docker image (tag: `ghcr.io/neverhuman/jansu`)
```

The artifacts must be reproducible and pass `cargo check`.
The memory-only build lane must stay green as a separate regression:

```sh
cargo build --bin jansu --no-default-features --features dynostore
```

## 3. Test suite

Run the full test suite, including integration and fuzz tests:

```sh
just test                 # Unit & integration tests (nextest)
cargo test -p jansu-broker --test embedded_memory -- --test-threads=1
cargo test -p jansu-broker --test auth -- --test-threads=1
cargo test -p jansu-client --test memory_native -- --test-threads=1
cargo test -p jansu-storage --test memory_contract -- --test-threads=1
just fuzz-request-decode   # Fuzz the request‑decode target (optional, but recommended)
```

All tests must pass without warnings.

## 4. Security lane

Execute the security lane to ensure no new secrets or vulnerable dependencies are introduced:

```sh
./tools/security-lane.sh
```

The script runs Gitleaks and `cargo audit`.

## 4.1 Repair receipts

Use the smallest local rerun that matches the failure:

| Symptom | Rerun |
| --- | --- |
| Broker startup or in-process broker tests fail | `cargo test -p jansu-broker --test embedded_memory -- --test-threads=1` |
| Auth or SASL proof fails | `cargo test -p jansu-broker --test auth -- --test-threads=1` |
| Client memory-path proof fails | `cargo test -p jansu-client --test memory_native -- --test-threads=1` |
| Storage memory lifecycle proof fails | `cargo test -p jansu-storage --test memory_contract -- --test-threads=1` |
| Workflow or audit routing changes | `just score` |
| Release checklist changes | `just audit-check` |

If a rerun fails twice with the same symptom, stop and inspect the linked command output before changing code.

## 5. Release vetting

- **Docker image scan** – Run `docker scan ghcr.io/neverhuman/jansu` (or equivalent) and confirm no high‑severity findings.
- **Smoke test** – Deploy a fresh compose stack and run basic end‑to‑end checks:
- **Backups** – Confirm the current database and storage paths are restorable from the last known backup or snapshot.
- **Monitoring** – Verify the dashboard and alerting stack comes up cleanly and the broker metrics endpoint is reachable locally.
- **Rollback** – Record the previous release tag and verify the deployment can be reverted without changing persisted data.
- **Abuse controls** – Confirm public ports remain bound to localhost or an internal network only, and re-run the security lane after any compose changes.

### 5.1 Launch-gate evidence

Treat release readiness as an evidence bundle, not a verbal checklist. Before a tagged release, collect the following receipts under `target/jankurai/release/` and keep them with the release candidate:

| Gate | Command | Required receipt |
| --- | --- | --- |
| Full release bundle | `just release-evidence` | `target/jankurai/release/compose-config.txt`, `target/jankurai/release/compose-ps.json`, `target/jankurai/release/prometheus-ready.txt`, `target/jankurai/release/grafana-health.json`, `target/jankurai/release/db-ready.txt`, `target/jankurai/release/db-backup.sql`, `target/jankurai/release/data-backup.tgz` |
| Security | `just security-lane` | `target/jankurai/security/evidence.json` |
| Backup | `docker compose exec db pg_dump -U postgres postgres > target/jankurai/release/db-backup.sql` and `tar -czf target/jankurai/release/data-backup.tgz data/` | `target/jankurai/release/backup-evidence.md` with the dump and snapshot identifiers |
| Monitoring | `docker compose ps --format json > target/jankurai/release/compose-ps.json` and `curl -fsS http://127.0.0.1:9090/-/ready` | `target/jankurai/release/monitoring-evidence.md` with the Prometheus, Grafana, and broker health checks |
| Rollback | `git describe --tags --abbrev=0` and `docker compose down --remove-orphans --volumes` before a fresh `just jansu-up` replay | `target/jankurai/release/rollback-evidence.md` with the previous tag and restore notes |
| Abuse controls | `docker compose config > target/jankurai/release/compose-config.txt` and `docker compose port jansu 9092` | `target/jankurai/release/abuse-controls.md` showing that public ports stay on `127.0.0.1` or an internal network |

If any of those receipts are missing, do not publish the tag. Re-run the smallest failing command, attach the raw output, and only continue after the missing receipt exists.

```sh
just jansu-up   # Spin up the jansu stack
# (run a simple client against the broker, e.g., produce/consume a test topic)
just jansu-down
```

## 6. Budgets and stop conditions

- Any paid or externally metered job needs a written budget before it starts.
- Stop when the budget, timebox, or retry ceiling is reached.
- Do not proceed if rollback evidence, backup evidence, or security evidence is missing.
- Use `just security` and `docker compose down --remove-orphans --volumes` as the kill switch for bad local candidates.

## 6a. Launch-gate checklist

All gates must pass before tagging a release. Evidence artifacts listed below are produced by CI.

| Gate | Evidence artifact | CI step |
|---|---|---|
| Security scan | `target/jankurai/security/evidence.json` | `bash ops/ci/jankurai.sh` |
| Audit score ≥ 85 | `target/jankurai/repo-score.json` | `bash ops/ci/jankurai.sh` |
| Tests pass | `target/nextest/default/junit.xml` | `bash ops/ci/test.sh` |
| Rollback plan | `docs/exceptions/README.md` | manual review |
| Monitoring live | `compose.yaml` (Grafana + Prometheus) | `just ci` |
| Abuse controls | `jansu-auth/src/handshake.rs` (SASL) | code review |
| Cost budget documented | `docs/testing.md` §6 | this doc |

## 7. Publish (CI only)

When a tag matching `v*` is pushed, the CI workflow will:

1. Build and push the Docker image.
2. Create a GitHub release with the tagged version and attach the release binary.

---

## 8. Post‑release verification

- Verify the release assets are downloadable from the GitHub release page.
- Confirm the Docker image is available on GHCR.
- Run a quick sanity check against the published image:

```sh
docker run --rm ghcr.io/neverhuman/jansu --version
```

If any step fails, abort the release and address the issue before re‑trying.
