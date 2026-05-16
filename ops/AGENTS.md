<!-- jankurai:owner: ops -->
<!-- jankurai:proof-lane: security -->
<!-- jankurai:expiry: 2026-07-01 -->

# Ops Surface Ownership

This file declares the operational surface of the Jansu repository for jankurai
ownership tracking, audit lanes, and security review. It is intentionally placed
at `ops/AGENTS.md` so that future consolidation of operational concerns under a
single `ops/` tree has a canonical entry point. Until that consolidation lands,
the active operational surface lives at `.github/workflows/`, and this document
treats that directory as the authoritative alternate profile.

## Active Profile

- Profile name: `gha-workflows`
- Surface root: `.github/workflows/`
- Owner: Platform Engineering (`@ops`)
- Proof lane: `security`
- Profile expiry: `2026-07-01`
- Profile migration target: `ops/`

The expiry date is the calendar limit on this alternate profile. Before that
date, the workflows under `.github/workflows/` must be consolidated under a
dedicated `ops/` subtree (see Migration Plan, below), or the expiry date must
be renewed with explicit sign-off recorded in `AUDIT.md`. Audit runs after the
expiry without an extension or migration MUST fail the `security` lane.

## Files in Scope (Active Profile)

The following files are inside the `gha-workflows` profile and inherit the
ownership, proof lane, and prohibitions declared here:

- `.github/workflows/ci.yml`
- `.github/workflows/differential-kafka-lab.yml`
- `.github/workflows/release.yml`

Any newly added workflow file under `.github/workflows/` joins this profile
automatically. The reviewer of the introducing pull request is responsible for
recording the new file path in the next jankurai audit run.

## Owner

- Group: Platform Engineering
- GitHub handle placeholder: `@ops`
- Escalation path: Platform Engineering on-call, then repository administrators.
- Review cadence: monthly walk-through of every workflow listed above, plus an
  unscheduled review whenever a workflow is added or whenever a security
  advisory affects a pinned action.

The handle `@ops` is a placeholder. Once a real GitHub team handle is created,
the owner section of this document and the jankurai audit policy must both be
updated in the same change set so ownership matches reality.

## Proof Lane

The proof lane is `security`. Every change to a file in scope must run, and
pass, the `security` lane before merge. The `security` lane verifies, at a
minimum:

1. Every third-party action is referenced by an immutable Git SHA, not by a
   branch name or floating tag.
2. No workflow echoes a secret to standard output or to the GitHub Actions
   log group, whether directly (`echo "$TOKEN"`) or indirectly through
   diagnostic commands such as `env`, `set`, or `printenv`.
3. No workflow performs a force push, a non-fast-forward push, or a tag
   reassignment against any protected branch.
4. `permissions:` is set at the workflow or job level and uses the principle
   of least privilege (`contents: read` unless write is genuinely required).
5. Reusable workflows from other repositories are pinned by SHA and listed in
   `docs/security/agent-tool-supply.md` under the trusted action catalog.

## Forbidden Patterns

Reviewers must reject any pull request that introduces, or fails to remove,
any of the following patterns inside a file in scope:

- **Mutable action references.** Any `uses:` line that resolves to a branch
  name, version tag, or other mutable ref is forbidden. All references must use
  the full 40-character commit SHA. The action's human-readable version may be
  recorded in a trailing comment, for example
  `uses: actions/checkout@1d96c772d19495a3b5c517cd2bc0cb401ea0529f # v6`.
- **Secret echo.** Any direct or indirect dump of a secret variable. This
  includes `run: echo ${{ secrets.X }}`, `run: env`, `run: set`,
  `uses: hmarr/debug-action@*`, or appending secrets to a log artifact.
- **Force-push automation.** Workflows that call `git push --force`,
  `git push --force-with-lease`, `git push --mirror`, or that delete and
  recreate a remote branch as a side effect of release automation. The
  release workflow is permitted to push new tags but never to overwrite or
  delete existing tags or branches.
- **Missing SHA pinning** on reusable workflows and container images. Both
  `uses: org/repo/.github/workflows/x.yml@ref` and
  `container: image:tag` must resolve to a SHA-pinned form.
- **Plaintext credential interpolation** into a `run:` block. Use the
  `env:` key with a job-level secret reference so the credential is never
  emitted into the shell history or into the YAML expansion log.
- **Self-hosted runners** without a documented isolation boundary. If a
  self-hosted runner is introduced, it must be paired with a corresponding
  entry in `docs/security/agent-tool-supply.md` and reviewed by Platform
  Engineering before merge.

## Required Patterns

- Every workflow declares a top-level `permissions:` block.
- Every workflow uses `concurrency:` to cancel superseded runs on the same
  ref, to prevent races between two simultaneous merges to `main`.
- Every job that uploads an artifact uses a deterministic artifact name so
  the audit lane can correlate evidence across runs.
- Every job that consumes secrets restricts the secret list to the minimum
  surface that job needs.

## Negative Tests

The audit lane runs the following non-passing checks against the active
profile and confirms each one returns the matching failure code:

1. Introduce a workflow line `uses: actions/checkout@v6` (no SHA). The audit
   lane must fail with `pinning_violation`.
2. Introduce a workflow line `run: echo "${{ secrets.GHCR_TOKEN }}"`. The
   audit lane must fail with `secret_echo`.
3. Introduce a workflow step that calls `git push --force origin main`. The
   audit lane must fail with `force_push_to_protected_branch`.

These negative tests are scripted in the `security` lane and are themselves
covered by the same proof requirements as the workflows under audit.

## Cross-References

- `docs/security/agent-tool-supply.md` lists the pinned versions of every
  third-party tool, action, and CLI that may appear in the active profile.
- `docs/release/release-readiness.md` defines the launch-gate dependencies
  of the release workflow.
- `docs/ops/cost-budget.md` enumerates the CI-minute and storage budgets
  that constrain how aggressively the workflows can be scheduled.

## Migration Plan

Within the expiry window ending `2026-07-01`, the workflows currently in
`.github/workflows/` will be consolidated under a dedicated `ops/`
subtree to provide a single canonical operational surface. The migration
will proceed in three steps and is intentionally non-destructive: each step
leaves the previous behaviour observable until the next step lands.

1. **Create `ops/workflows/`**, a normal directory that holds reusable
   composite actions and shell helpers extracted from the current top-level
   workflow files. `.github/workflows/*.yml` keeps the GitHub-required path
   but reduces to thin entry points that call into `ops/workflows/`.
2. **Introduce `ops/runbooks/`** for human-facing operational procedures
   (release, incident response, on-call playbooks). Every runbook references
   the workflow it complements.
3. **Promote `ops/AGENTS.md` to the primary profile** in
   `agent/audit-policy.toml`. The `gha-workflows` profile is then archived,
   not removed, so historical audit runs against earlier commits remain
   reproducible.

If the migration cannot be completed by the expiry date, the owner must
either renew the expiry with an explicit AUDIT entry or accept the
audit-lane failure that will otherwise occur the first time `security` is
run against the affected commit. Renewal entries must include a new
expiry date and a brief justification, and they must be reviewed by an
agent other than the one that proposed the renewal.

## Change Control

A change to this file requires the same review as a change to any workflow
in the active profile: at least one Platform Engineering reviewer plus a
green `security` lane. Trivial edits (typo fixes, link corrections) may be
approved by a single reviewer provided they cite the trivial-change rule
in the pull request description.
