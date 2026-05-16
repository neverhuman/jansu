# Launch Readiness Gate

This document is the launch gate for every Jansu launch. It enumerates
the checks that must pass before a launch tag is pushed, the procedures
that run during the launch itself, and the fallback path used when one
of the gates is blocked. The gate is binary: every check must be green
for the launch to proceed; a single red check stops the train.

The launch-readiness owner is Platform Engineering, with Storage
Engineering co-sign for any launch that includes a database migration
and Protocol Engineering co-sign for any launch that changes a
contract artefact.

## Launch Cadence

Jansu launchs on a weekly cadence by default, pushed from `main` on
Wednesdays. Out-of-band launchs are permitted for security advisories
and for incident remediation; they follow the same gate, with the
quarterly review reconciliation skipped if it would block a critical
fix.

## Gate Summary

A launch proceeds only when every row in the table below is green:

| Gate | Owner | Evidence path |
|---|---|---|
| Backups verified | Storage Engineering | `just launch-evidence` artefact |
| Monitoring green | Platform Engineering | Prometheus and Grafana panels |
| Rollback procedure rehearsed | Platform Engineering | This file, "Rollback procedure" |
| Abuse controls present | Platform Engineering | Section "Abuse controls" |
| ACL audit run | Platform Engineering | `docs/security/authz-matrix.md` |
| Security lane green within 7 days | Platform Engineering | `.github/workflows/ci.yml` security job |
| Tooling catalogue current | Platform Engineering | `docs/security/agent-tool-supply.md` |
| Cost-budget caps observed | Platform Engineering | `docs/ops/cost-budget.md` |
| Contracts audit lane green | Protocol Engineering | `contracts/AGENTS.md` proof lane |
| Database doctor lane green | Storage Engineering | `db/AGENTS.md` proof lane |

The launch manager fills in the table on the launch pull request and
records the artefact paths or run identifiers next to each row.

## 1. Backups Verified

Backups are verified by running `just launch-evidence` against the
staging environment before the launch. The recipe is reserved for the
launch flow and produces an artefact archive containing:

1. A backup snapshot of the PostgreSQL `jansu` schema, taken with
   `pg_dump` against the staging instance.
2. A backup snapshot of the libSQL store for any deployment that uses
   the SQLite backend.
3. A listing of the S3 bucket contents at the moment of the snapshot
   (object key, byte size, last-modified timestamp).
4. The output of `cargo run -p jansu --bin jansu -- topic list` against
   each backend, so the consumer-group offsets and topic configurations
   are captured.
5. A SHA-256 digest of every file in the archive.

The artefact is uploaded to the launch-evidence bucket and the
digest list is recorded in the launch pull request. A launch that
proceeds without a green launch-evidence artefact is not a valid
Jansu launch.

## 2. Monitoring Green

The launch gate requires the development-environment monitoring stack
to be green at the moment the launch pull request is approved. The
stack is declared in `compose.yaml`:

- Prometheus at `compose.yaml:86`, serving on port 9090. The launch
  manager confirms that the broker's `up` metric, the
  `process_resident_memory_bytes` gauge, and the per-request latency
  histograms are all returning values from a recent scrape.
- Grafana at `compose.yaml:4`, serving on port 3000. The launch
  manager visits the home dashboard
  (`/etc/dashboards/home.json` per the bind mount at
  `compose.yaml:19`) and confirms that no panel is in an error or
  no-data state.
- Jaeger at `compose.yaml:78`, serving on port 16686. The launch
  manager confirms that a sample trace for a Produce request and a
  sample trace for a Fetch request both appear within a five-minute
  window.

A production deployment that uses a managed Prometheus or managed
tracing backend substitutes the managed dashboards for the local
ones; the substitution must be recorded in the launch pull request.

## 3. Rollback Procedure

Every launch ships with a rollback playbook. The playbook below is
the default; a launch with database migrations or contract changes
extends the playbook with the migration-reverse or contract-rollback
step recorded in `db/AGENTS.md` or `contracts/AGENTS.md`.

### Default rollback steps

1. **Pause traffic.** Drain the broker's listener by removing it from
   the load balancer or by setting the advertised listener URL to a
   reserved sinkhole address. Producers and consumers should reach a
   `Coordinator unavailable` state, not a TCP error.
2. **Confirm pause.** Tail the broker's `tracing` log and confirm no
   new request frames are being decoded.
3. **Pin the image.** Re-tag the most recently verified launch image
   in the registry so subsequent restarts pick it up. The image
   reference lives in the production deployment manifest (out of
   tree); the launch manager records the previous SHA in the
   rollback log.
4. **Restart with the previous image.** Restart the broker container
   with the previous image SHA. Wait for the readiness probe to pass.
5. **Restore listener.** Add the broker back to the load balancer or
   restore the advertised listener URL.
6. **Verify.** Run the post-launch smoke test (`just smoke` or the
   equivalent CI invocation) against the restored broker.
7. **Record.** File a postmortem entry under
   `phase-logs/attempts/launch-rollback/` describing the trigger,
   the timeline, and any data loss.

The rollback playbook is rehearsed once per quarter as part of the
launch-readiness review. The rehearsal is recorded in `AUDIT.md`.

## 4. Abuse Controls

The launch gate verifies that the broker enforces the following
abuse-control surfaces.

### Rate limits

At the time of writing, Jansu does not enforce a client-side rate
limit beyond the protocol-level throttle field. The `throttle_time_ms`
response field is wired up in
`jansu-broker/src/coordinator/group/administrator/forming.rs` at
lines 305, 368, 376, 443, 460, 487, 518, 625, 685, and 698, all
returning zero. A non-zero throttle would slow misbehaving consumers
down without dropping their connection.

This absence is recorded so the launch gate makes it visible:
production deployments must enforce rate limits at the load-balancer
layer or via a sidecar until the broker grows native rate limiting.
The remediation item is tracked in `AUDIT.md` and the gate flags a
warning until it lands.

### ACL audit

A full ACL audit is required at every launch. The audit:

1. Iterates every (principal, resource, operation) combination defined
   in `docs/security/authz-matrix.md` and confirms each cell still
   matches the matrix.
2. Confirms that the matrix's source files
   (`jansu-sans-io/src/acl.rs` and `jansu-sans-io/src/resource.rs`)
   have not been edited without a corresponding update to the matrix.
3. Confirms that the negative tests
   (`heartbeat_from_unknown_member_returns_error` at
   `jansu-broker/src/coordinator/group/administrator/tests.rs:1569`,
   `leave_unknown_member_returns_per_member_error` at
   `jansu-broker/src/coordinator/group/administrator/tests.rs:2655`,
   and the `lifecycle` baseline at
   `jansu-broker/src/coordinator/group/administrator/tests.rs:108`)
   still pass on the launch candidate.

The audit produces a single-page summary that the launch manager
attaches to the launch pull request.

### Per-connection limits

The wire-level cap on inbound bytes is `MESSAGE_MAX_SIZE` declared at
`jansu-sans-io/src/de.rs:31` (one gibibyte). The launch gate confirms
the cap has not been raised without a security review. The cap is
also enumerated in `docs/security/input-boundary.md`.

## 5. Security Lane Green Within Seven Days

The `security` job inside `.github/workflows/ci.yml` must have a green
run against the launch candidate no more than seven calendar days
before the launch tag is pushed. The launch manager records the
workflow run identifier on the launch pull request. A run older than
seven days is treated as a missing run; the lane must be re-triggered
before the launch proceeds.

## 6. Tooling Catalogue Current

The launch manager opens `docs/security/agent-tool-supply.md` and
confirms:

- The Jankurai version recorded there matches the version installed
  on the launch host.
- Every workflow `uses:` line is SHA-pinned.
- No quarterly review is overdue.

If a quarterly review is overdue, the launch proceeds only if the
launch is itself a security-advisory launch; otherwise the review
must run first.

## 7. Cost-Budget Caps Observed

The launch manager checks the rolling CI-minute total against the
caps recorded in `docs/ops/cost-budget.md`. A launch does not
proceed if either the CI-minute hard cap or the S3 hard cap has been
crossed in the current month, unless the launch is itself the
remediation for the breach.

## 8. Contracts Audit Lane

The launch gate confirms the `contracts-audit` lane defined in
`contracts/AGENTS.md` is green for the launch-candidate commit.
Any change to a contract artefact under `contracts/` that has not
been verified by the lane is treated as a launch-blocking issue.

## 9. Database Doctor Lane

The launch gate confirms the `db-doctor` lane defined in
`db/AGENTS.md` is green for the launch-candidate commit. Every
migration that lands in the launch window must have:

- A green forward-migration test run on every backend matrix entry.
- A documented rollback procedure under `db/migrations/<backend>/reverse/`
  for any destructive migration.
- An entry in `docs/db/destructive-delete-proof.md` for any
  destructive operation, which is the canonical evidence file that
  records pre- and post-row counts and the rollback plan.

## Launch Procedure

When every gate is green, the launch proceeds as follows:

1. The launch manager opens the launch pull request that bumps the
   workspace `version` in `Cargo.toml`, updates the changelog, and
   records each gate's evidence on the pull request body.
2. After the pull request is reviewed and merged, the launch manager
   pushes a tag matching `v*` (per the `launch.yml` trigger at
   `.github/workflows/launch.yml:3-5`).
3. The `launch.yml` workflow runs and publishes the workspace to
   crates.io.
4. The launch manager monitors the production rollout via the
   monitoring panels referenced above.
5. After 24 hours of stable production telemetry, the launch manager
   marks the launch as cleared in `AUDIT.md`.

## Launch Fallback When Budgets Are Breached

When the cost-budget hard cap is crossed and the launch workflow
refuses to run, the operator may produce a launch artefact through
the following manually approved path. Invoking the fallback requires
sign-off from Platform Engineering and Storage Engineering. The
sign-off is recorded in the launch-readiness checklist.

1. Build the launch binary on an approved developer workstation by
   running `just launch` (recipe at `justfile:23`). The recipe
   builds with the full feature set
   (`delta,dynostore,iceberg,libsql,parquet,postgres,slatedb`).
2. Verify the launch binary against the staging environment using
   the standard smoke-test corpus.
3. Compute SHA-256 digests for every artefact and record them in the
   launch pull request body.
4. Publish the artefacts to the launch-evidence bucket by hand,
   labelled with the fallback indicator and the date.
5. File a remediation pull request that addresses the budget breach;
   the next regular launch window cannot open until the remediation
   has landed.

The fallback is intentionally a high-friction path: every step
requires an explicit human approval, and every artefact is recorded
with a separate label so the audit trail distinguishes a fallback
launch from a regular launch.

## Post-Launch Verification

After every launch (regular or fallback):

1. Re-run `just launch-evidence` against the production environment
   and compare the result to the pre-launch artefact. A non-empty
   diff is a launch-quality alert.
2. Confirm Prometheus continues to scrape the broker (no
   `up == 0` for at least 30 minutes after the launch).
3. Confirm the consumer-group offsets are advancing in the Grafana
   panel for consumer lag.
4. File the launch log entry under
   `phase-logs/attempts/launch/`.

## Cross-References

- `ops/AGENTS.md` declares the operational profile that the launch
  workflow inherits.
- `contracts/AGENTS.md` and `db/AGENTS.md` declare the per-surface
  proof lanes the launch depends on.
- `docs/security/authz-matrix.md` is the canonical principal-by-
  resource-by-operation matrix consulted by the ACL audit step.
- `docs/security/input-boundary.md` enumerates the input sinks whose
  caps the launch gate verifies.
- `docs/security/agent-tool-supply.md` is the pinned-tool catalogue
  the launch manager consults at gate 6.
- `docs/ops/cost-budget.md` defines the caps and stop conditions
  that the launch gate enforces at gate 7.

## Change Control

A change to this file requires review by Platform Engineering and
the launch manager. Trivial edits (typo fixes, link corrections)
may be approved by a single reviewer; substantive changes (adding,
removing, or reordering a gate) require both reviewers plus a
green `security` lane.
