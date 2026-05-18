# Testing Checklist

This document covers testing procedures and CI lanes for Jansu.

---

## 1. Preparation steps

- **Version bump** – Update the version in `Cargo.toml` (and any other manifest) and commit the change.
- **Changelog** – Add an entry to `CHANGELOG.md` summarising notable changes, breaking changes, and migration notes.
- **Documentation** – Ensure `README.md` and API docs are up‑to‑date (`just doc`).

## 2. Build artifacts

```sh
just build-dynostore      # Memory-only proof lane for embedded broker work
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

## 5. Launch gate evidence

Release readiness is documented in [docs/launch/launch-checklist.md](docs/launch/launch-checklist.md).
The launch evidence recipe is `just launch-evidence`, which writes the launch-gate receipts under `target/jankurai/launch/`:

- `compose-config.txt`
- `compose-ps.json`
- `prometheus-ready.txt`
- `grafana-health.json`
- `db-ready.txt`
- `db-backup.sql`
- `data-backup.tgz`
- `backup-evidence.md`
- `monitoring-evidence.md`
- `rollback-evidence.md`
- `abuse-controls.md`

Do not treat launch readiness as satisfied until those artifacts exist and match the launch checklist.

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
