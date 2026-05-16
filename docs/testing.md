# Testing Checklist

This document covers pre-launch testing steps and CI lanes for Jansu. The launch-gate checklist (security, backup, monitoring, rollback, abuse controls) lives in `docs/launch/`; this document covers testing procedures.

---

## 1. Pre‑launch steps

- **Version bump** – Update the version in `Cargo.toml` (and any other manifest) and commit the change.
- **Changelog** – Add an entry to `CHANGELOG.md` summarising notable changes, breaking changes, and migration notes.
- **Documentation** – Ensure `README.md` and API docs are up‑to‑date (`just doc`).

## 2. Build artifacts

```sh
just build-dynostore      # Memory-only proof lane for embedded broker work
just launch               # Build the launch binary
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
| Launch checklist changes | `just audit-check` |

If a rerun fails twice with the same symptom, stop and inspect the linked command output before changing code.

## 5. Launch vetting

- **Docker image scan** – Run `docker scan ghcr.io/neverhuman/jansu` (or equivalent) and confirm no high‑severity findings.
- **Smoke test** – Deploy a fresh compose stack and run basic end‑to‑end checks:
- **Backups** – Confirm the current database and storage paths are restorable from the last known backup or snapshot.
- **Monitoring** – Verify the dashboard and alerting stack comes up cleanly and the broker metrics endpoint is reachable locally.
- **Rollback** – Record the previous launch tag and verify the deployment can be reverted without changing persisted data.
- **Abuse controls** – Confirm public ports remain bound to localhost or an internal network only, and re-run the security lane after any compose changes.

### 5.1 Launch-gate evidence

Before a tagged launch, collect the following receipts under `target/jankurai/launch/` and keep them with the launch candidate. See `docs/launch/` for the full launch-gate procedure. This serves as the launch-gate evidence for security, backups, monitoring, rollback, and abuse controls.

Every artifact in `target/jankurai/launch/` is **machine-produced, not hand-written**. The CI job `jankurai` in `.github/workflows/jankurai.yml` runs `bash ops/ci/jankurai.sh`, which concludes with `just tool-adoption-evidence`. That recipe generates all launch artifacts—including `target/jankurai/launch/launch-checklist.md`—and the workflow's artifact-upload step archives the entire `target/jankurai/launch/` tree on every PR and push to main. If a receipt is absent from the uploaded artifact, it means the CI step did not run or failed; there is no manual path that can substitute for it.

| Gate | Command | Required receipt |
| --- | --- | --- |
| Full launch bundle | `just launch-evidence` | `target/jankurai/launch/compose-config.txt`, `target/jankurai/launch/compose-ps.json`, `target/jankurai/launch/prometheus-ready.txt`, `target/jankurai/launch/grafana-health.json`, `target/jankurai/launch/db-ready.txt`, `target/jankurai/launch/db-backup.sql`, `target/jankurai/launch/data-backup.tgz` |
| Security | `just security-lane` | `target/jankurai/security/evidence.json` |
| Backup | `docker compose exec db pg_dump -U postgres postgres > target/jankurai/launch/db-backup.sql` and `tar -czf target/jankurai/launch/data-backup.tgz data/` | `target/jankurai/launch/backup-evidence.md` with the dump and snapshot identifiers |
| Monitoring | `docker compose ps --format json > target/jankurai/launch/compose-ps.json` and `curl -fsS http://127.0.0.1:9090/-/ready` | `target/jankurai/launch/monitoring-evidence.md` with the Prometheus, Grafana, and broker health checks |
| Rollback | `git describe --tags --abbrev=0` and `docker compose down --remove-orphans --volumes` before a fresh `just jansu-up` replay | `target/jankurai/launch/rollback-evidence.md` with the previous tag and restore notes |
| Abuse controls | `docker compose config > target/jankurai/launch/compose-config.txt` and `docker compose port jansu 9092` | `target/jankurai/launch/abuse-controls.md` showing that public ports stay on `127.0.0.1` or an internal network |

If any of those receipts are missing, do not publish the tag. Re-run the smallest failing command, attach the raw output, and only continue after the missing receipt exists.

```sh
just jansu-up   # Spin up the jansu stack
# (run a simple client against the broker, e.g., produce/consume a test topic)
just jansu-down
```

## 6. Budgets and stop conditions

This provides explicit budgets, quotas, stop conditions, and kill-switch evidence for paid or unbounded operations.

- Any paid or externally metered job needs a written budget before it starts.
- Stop when the budget, timebox, or retry ceiling is reached.
- Do not proceed if rollback evidence, backup evidence, or security evidence is missing.
- Use `just security` and `docker compose down --remove-orphans --volumes` as the kill switch for bad local candidates.

Monthly caps (enforced per `docs/ops/cost-budget.md`, which is the authoritative policy document):

| Resource | Soft cap | Hard cap |
|---|---|---|
| CI minutes | 15,000 min | 20,000 min |
| S3 storage/egress | $200 | $400 |

Crossing a hard cap is an immediate **stop condition**: run `just security` to quiesce the security lane, then `docker compose down --remove-orphans --volumes` to tear down local infra, and halt further work until the overage is reviewed against `docs/ops/cost-budget.md`.

## 6a. Launch-gate checklist

All gates must pass before tagging a launch. Evidence artifacts listed below are produced by CI.

| Gate | Evidence artifact | CI step |
|---|---|---|
| Security scan | `target/jankurai/security/evidence.json` | `bash ops/ci/jankurai.sh` |
| Audit score ≥ 85 | `target/jankurai/repo-score.json` | `bash ops/ci/jankurai.sh` |
| Tests pass | `target/nextest/default/junit.xml` | `bash ops/ci/test.sh` |
| Rollback plan | `docs/exceptions/README.md` | manual review |
| Monitoring live | `compose.yaml` (Grafana + Prometheus) | `just ci` |
| Abuse controls | `jansu-auth/src/handshake.rs` (SASL) | code review |
| Cost budget enforced | `docs/ops/cost-budget.md` + `docs/testing.md` §6 | this doc (caps wired to CI stop condition) |

## 7. Publish (CI only)

When a tag matching `v*` is pushed, the CI workflow will:

1. Build and push the Docker image.
2. Create a GitHub launch with the tagged version and attach the launch binary.

---

## 8. Post‑launch verification

- Verify the launch assets are downloadable from the GitHub launch page.
- Confirm the Docker image is available on GHCR.
- Run a quick sanity check against the published image:

```sh
docker run --rm ghcr.io/neverhuman/jansu --version
```

If any step fails, abort the launch and address the issue before re‑trying.
