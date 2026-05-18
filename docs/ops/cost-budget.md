# Cost Budget

This document records the per-month spending caps for the Jansu
project's CI, object-storage, and container-build footprint. The audit
lane (`security`) consults these caps when it evaluates a workflow
change; a pull request that increases a cap requires the same proof
discipline as a pull request that introduces a new workflow.

The numbers in this document are working estimates derived from the
shapes of the workflows currently checked into `.github/workflows/`
and the storage volumes declared in `compose.yaml`. They are not
billing invoices and must be reconciled against actual billing at
each quarterly review.

## Currency and Accounting Period

All monetary figures are in United States dollars. All time figures
are wall-clock minutes per calendar month. The accounting period
starts on the first day of the month and runs to the first day of
the following month.

## 1. CI Minutes (GitHub Actions)

The workflows under `.github/workflows/` consume CI minutes whenever
they run.

### `ci.yml`

- Triggers: `push` to `main`, `push` of any tag matching `v*`, and
  `pull_request` against `main` (per `.github/workflows/ci.yml:6-13`).
- Matrix size: three PostgreSQL major versions (16, 17, 18, per
  `.github/workflows/ci.yml:27-29`) crossed with one operating system
  row (`ubuntu-latest`). Three parallel jobs per trigger.
- Per-job duration estimate: 45 minutes (`just ci` plus build, plus
  the full `just test` invocation including the cross-backend test
  suite).
- Per-trigger cost estimate: `3 jobs * 45 minutes = 135 minutes`.
- Triggers per month estimate: 60 pull-request events plus 25
  push-to-main events plus 4 tag pushes, for a working assumption of
  90 triggers per month.
- Monthly estimate: `90 triggers * 135 minutes = 12,150 minutes`.

### `differential-kafka-lab.yml`

- Triggers: `workflow_dispatch` and a path-filtered `pull_request`
  trigger (per `.github/workflows/differential-kafka-lab.yml:3-15`).
- Matrix size: one job, `ubuntu-latest`.
- Per-job duration estimate: 30 minutes (capped at 45 by the workflow
  `timeout-minutes` setting).
- Triggers per month estimate: 20 (path filter limits the trigger
  surface to compatibility-bearing changes).
- Monthly estimate: `20 triggers * 30 minutes = 600 minutes`.

### `release.yml`

- Triggers: `push` of a tag matching `v*` (per
  `.github/workflows/release.yml:3-5`).
- Matrix size: one job, `ubuntu-latest`.
- Per-job duration estimate: 15 minutes (workspace publish to
  crates.io after build and check).
- Triggers per month estimate: 4 (one release per week, rounded up).
- Monthly estimate: `4 triggers * 15 minutes = 60 minutes`.

### CI total

- Working estimate: `12,150 + 600 + 60 = 12,810 CI minutes per month`.
- Soft cap: 15,000 CI minutes per month.
- Hard cap: 20,000 CI minutes per month.

A month that crosses the soft cap triggers an audit note and a
remediation pull request that either tightens the trigger surface or
shortens the per-job duration. A month that crosses the hard cap is
treated as a stop condition; see the Stop Conditions section below.

## 2. Object Storage (S3 and MinIO)

Object storage costs accrue in two places: the production object
store (S3) and the development-time MinIO container that the audit
lane brings up via `compose.yaml`.

### Production S3

- Bucket purpose: durable log of every Kafka topic when the broker is
  configured with `--storage-engine s3://<bucket>/`.
- Working volume estimate: 200 GB per month of new data, with a
  rolling 30-day retention horizon for the default topic policy and
  a 365-day horizon for compacted topics. Total durable footprint
  ranges from 200 GB to 2.4 TB depending on the producer load.
- Working transfer estimate: 50 GB egress per month (mostly consumer
  fetches that overflow the broker's in-memory cache).
- Cost estimate (US East 1, standard tier, May 2026 price list):
  `2.4 TB * 0.023 USD/GB-month = 56.40 USD storage` plus
  `50 GB * 0.09 USD/GB = 4.50 USD egress = 60.90 USD/month`.
- Soft cap: `200 USD/month`.
- Hard cap: `400 USD/month`.

### Development MinIO

- Container declared at `compose.yaml:40-54` and bound to a
  `minio` named volume declared at `compose.yaml:135-136`.
- Local-disk footprint: 5 GB per developer machine, refreshed
  whenever `just ci` recreates the container.
- Cost estimate: zero (local disk).
- Cap: not applicable; the audit lane checks only that the MinIO
  service is restricted to localhost (`compose.yaml:44-46`).

## 3. Container Build Hours

- Image registry: `${JANSU_IMAGE}` (`ghcr.io/jansu-io/jansu` per
  `.github/workflows/ci.yml:49`).
- Build cadence: every push to `main` (`ci.yml` trigger), plus every
  release tag.
- Per-build duration estimate: 20 minutes (full release-profile
  build of the workspace, plus image-layer assembly).
- Builds per month estimate: 25 push-to-main events plus 4 tag
  pushes, for 29 builds.
- Monthly estimate: `29 builds * 20 minutes = 580 build-minutes per
  month`.
- Image retention: indefinite for tagged releases, 90 days for
  branch builds.
- Storage cost estimate (GHCR, standard tier, May 2026): negligible
  at the current image size (~150 MB per release) and retention
  schedule, but tracked here so it remains visible.
- Soft cap: 1,000 build-minutes per month.
- Hard cap: 1,500 build-minutes per month.

## 4. Iceberg Catalog Hours

- Service: `quay.io/lakekeeper/catalog:v0.8.5` (declared at
  `compose.yaml:94` and pinned in CI at
  `.github/workflows/ci.yml:42`).
- Working assumption: the catalog runs only inside CI jobs; the
  cost is included in the CI-minute estimate above. No additional
  budget line is required while there is no production deployment
  of the Iceberg catalog.

## 5. Telemetry Backends

The audit lane runs the broker against the local-development
telemetry stack declared in `compose.yaml`:

- `prometheus` at `compose.yaml:86` (image `prom/prometheus:v3.1.0`),
  serving on port 9090.
- `grafana` at `compose.yaml:4` (image `grafana/grafana:11.5.1`),
  serving on port 3000.
- `jaeger` at `compose.yaml:78` (image
  `jaegertracing/all-in-one`), serving on ports 16686, 4317, 4318,
  5778, and 9411.

These services run inside CI and on developer machines only. They do
not contribute to the production cost ledger; the cost-budget cap is
nil. A future production deployment must add a row for managed
Prometheus and managed tracing.

## Aggregated Working Estimate

| Line item | Monthly working estimate | Soft cap | Hard cap |
|---|---|---|---|
| CI minutes | 12,810 | 15,000 | 20,000 |
| S3 storage and egress | 60.90 USD | 200 USD | 400 USD |
| Container build minutes | 580 | 1,000 | 1,500 |
| Telemetry (local only) | 0 USD | 0 USD | 0 USD |

The aggregated working estimate sits comfortably under every soft
cap. The point of the caps is to detect a regression in the workflow
shape (a runaway matrix, an accidental forever-loop in a test, a
log-blowout that fills MinIO) before it reaches the billing surface.

## Stop Conditions

A budget breach is a "stop loud" event. The audit and proof lanes
must fail with a documented, actionable message so an operator can
intervene without searching for context.

When the CI-minute soft cap is crossed in a month:

1. The `security` lane emits a warning that names the offending
   workflow, the cumulative minutes consumed, and the trigger
   pattern that produced the spike.
2. The next pull request that touches a file under
   `.github/workflows/` must include a remediation step that either
   tightens the trigger surface, shortens the per-job duration, or
   moves an expensive job behind a `workflow_dispatch` gate.
3. The breach is logged in `AUDIT.md` so the quarterly review
   includes it.

When the CI-minute hard cap is crossed in a month:

1. The `security` lane fails closed for every pull request until a
   remediation pull request has landed.
2. The release workflow refuses to run; the operator must invoke the
   fallback procedure in `docs/release/release-readiness.md` to
   produce a release artefact through a manually approved path.
3. The breach is treated as an incident: an entry is added under
   `phase-logs/attempts/incident/` and the on-call Platform
   Engineering rotation is paged.

When the S3 soft cap is crossed in a month, the audit lane emits a
warning and proposes a retention-window tightening pull request.
When the S3 hard cap is crossed, the audit lane fails the
`security` lane on any pull request that touches the storage
backend until the operator either raises the cap (with a recorded
justification) or trims the retention window.

When the container-build cap is crossed, the remediation is to move
the build behind a `workflow_dispatch` gate so it is not triggered
on every push.

## Fallback Path

The fallback procedure when a hard cap blocks normal operation lives
in `docs/release/release-readiness.md` under the section titled
"Release fallback when budgets are breached". That section describes:

- How to produce a release artefact from a developer workstation when
  the release workflow refuses to run.
- How to record the fallback artefact in `AUDIT.md` so the audit
  trail remains complete.
- How to file the corresponding remediation pull request before the
  next regular release window.

The fallback is intentionally not silent: invoking it requires
sign-off from Platform Engineering and Storage Engineering, and the
sign-off is recorded in the release-readiness checklist.

## Review Cadence

This file is reviewed quarterly, in the same review cycle as
`docs/security/agent-tool-supply.md`. The review:

1. Reconciles the working estimates against the actual billing for
   the past three months.
2. Updates the per-line estimates if the workflow shape has changed.
3. Tightens the caps if the actuals consistently sit well below the
   current caps, or loosens them (with justification) if the
   workflow shape has structurally expanded.
4. Records the review date and the reviewer in `AUDIT.md`.

Out-of-band reviews are required whenever a new workflow file lands,
whenever a new container image is added to `compose.yaml`, and
whenever a production-grade telemetry backend is introduced.

## Cross-References

- `ops/AGENTS.md` defines the active operational profile and the
  proof-lane requirements that apply to changes in
  `.github/workflows/`.
- `docs/security/agent-tool-supply.md` records the pinned versions
  of every action and container image referenced from the workflow
  files; a cap breach often correlates with an unpinned upgrade.
- `docs/release/release-readiness.md` defines the fallback path used
  when a hard cap blocks normal operation.
