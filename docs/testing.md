# Testing

Use the smallest proof lane that matches the changed surface, then broaden only
when the change crosses a contract boundary.

## Core Lanes

- `just fmt` - workspace formatting.
- `just check` - workspace type check with all targets and features.
- `just clippy` - workspace clippy with warnings denied.
- `just test` - full nextest and doc test lane.
- `just compatibility-contract` - ledger and advertised API contract proof.
- `just score` - Jankurai audit gate and score artifacts.
- `just fast` - local syntax, formatting, audit-helper, and workflow lint lane.

## Evidence Receipts

- Observability: `cargo test -p jansu-otel --all-features` verifies telemetry
  helpers; broker changes that alter spans or metrics also record the focused
  command in the attempt log.
- Exception handling: service and broker changes must preserve typed errors or
  structured tracing fields that an agent can route without reading free-form
  logs.
- Release readiness: `just release-check`, `just security`, `just score`, and
  `just compatibility-contract` are the minimum release evidence set; they
  write `target/release/`, `target/jankurai/security/`, `agent/repo-score.json`,
  `agent/repo-score.md`, and the compatibility ledger-backed artifacts described
  in `docs/release.md`.
- Cost budget: long-running Docker, differential Kafka, and broad matrix checks
  must follow the soft and hard caps in `docs/ops/cost-budget.md`; record the
  intended command, stop condition, and artifact path in the attempt log before
  starting paid or unbounded work.  Each cost surface has a monthly quota (the
  soft cap) and a kill switch (the hard cap) — the budget is the hard cap, the
  stop condition is the soft cap, and crossing the kill switch trip point halts
  the release/migration lane.
- Performance and concurrency: benchmark or soak changes must record the
  focused command, target profile, and artifact path before broadening.
- Human review: generated protocol, ACL, release, and compatibility artifacts
  require the attempt log to name the reviewer, source file, proof command, and
  resulting artifact path.
- Repair receipts: if a proof lane fails, the attempt log should name the next
  rerun command and the residual risk instead of summarizing the failure in
  prose only.

## Phase Lanes

Phase work must use the focused commands in the relevant `tips/phases/*.md`
file and record the exact commands in the attempt log. Kafka compatibility
claims are valid only when `docs/compatibility/kafka-4.2-ledger.json` points to
the proof.

## Stop Conditions

Stop before broadening a failing lane when the failure is unrelated to the
current phase or audit item. Record the failing command and residual risk in the
attempt log instead of masking it with unrelated changes.
