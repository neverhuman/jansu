# Database Surface Ownership

This file declares ownership of the database surface that lives under `db/`.
The directory holds the schema migrations, the constraint and index
definitions, and the SQL helper artifacts that drive every persistent
storage backend Jansu supports. Because these files shape the on-disk
representation of broker state, every change is treated as a deployment
event with rollback obligations attached.

## Profile

- Profile name: `db-schema`
- Surface root: `db/`
- Owner: Storage Engineering (`@db`)
- Proof lane: `db-doctor`
- Trust boundary: on-disk and in-cluster persistent state
- Review cadence: every release-candidate cut, plus an unscheduled review
  whenever a destructive migration is proposed.

## Files in Scope

The `db/` tree holds the following families. Each family has its own
forward-only history and its own rollback evidence requirements:

- `db/migrations/postgres/` - PostgreSQL forward migrations. These mirror
  and supersede the seed schema at
  `etc/initdb.d/010-schema.sql`, with one numbered file per migration
  step. PostgreSQL is the reference backend; the audit lane validates
  every other backend against the shape this directory defines.
- `db/migrations/libsql/` - libSQL (SQLite) forward migrations. These
  feed the libSQL bootstrap path in
  `jansu-storage/src/lite.rs`. Diverging shapes between PostgreSQL and
  libSQL must be explicitly documented in the migration file's header.
- `db/migrations/slatedb/` - SlateDB key-prefix migrations. SlateDB is
  schemaless on disk, so these files declare the key namespaces and
  versioning conventions rather than DDL.
- `db/constraints/` - Constraint and trigger definitions extracted from
  the migration history so reviewers can read every active invariant in
  one place.
- `db/queries/` - Reference SQL used by the runtime, kept in sync with
  the per-backend SQL under `jansu-storage/src/sql/` (PostgreSQL),
  `jansu-storage/src/lite/` (libSQL), and `jansu-storage/src/pg/`
  (PostgreSQL adapter glue). The runtime files remain the source of
  truth for the bytes the broker actually sends; `db/queries/` is the
  human-facing version used for review.

Files outside these directories are not in the `db-schema` profile. Test
fixtures and local-development seed scripts belong under `etc/initdb.d/`
or under the storage crate's `tests/` tree, not under `db/`.

## Owner

- Group: Storage Engineering
- GitHub handle placeholder: `@db`
- Escalation path: Storage Engineering on-call, then the release manager
  named in `docs/release/release-readiness.md`.

The handle `@db` is a placeholder. When a real GitHub team handle is
provisioned, the handle here and in `agent/audit-policy.toml` must change
together. The owner is on the page for every destructive operation that
touches a file in scope.

## Proof Lane

The proof lane is `db-doctor`. Every change to a file in scope must
satisfy:

1. The migration is forward-only. A reverse migration may be supplied as
   a separate file under `db/migrations/<backend>/reverse/`, but the
   forward file is never edited after it lands on `main`.
2. Every migration file is numbered with a strictly-increasing prefix and
   carries a one-line description in its filename
   (`0042_add_consumer_offset_expiry.sql`).
3. The migration includes an `UP` section and, where a reverse migration
   exists, the reverse file includes a `DOWN` section with the same
   numeric prefix.
4. The migration is exercised against every backend in the cross-backend
   integration test suite under `jansu-broker/tests/`, including the
   PostgreSQL matrix (16, 17, 18) and the libSQL backend.
5. Every destructive change (drop column, drop table, truncate, delete by
   predicate, alter column type narrowing) carries a proof artifact at
   `docs/db/destructive-delete-proof.md` that records the row counts
   before and after, the rollback procedure, and the evidence that no
   production data is at risk.

## Forbidden Patterns

Reviewers must reject any pull request that:

- **Performs a destructive operation without a proof artifact.** No
  `DROP TABLE`, `DROP COLUMN`, `TRUNCATE`, `ALTER COLUMN ... TYPE`
  (when narrowing), or `DELETE FROM ... WHERE ...` may land without a
  matching entry in `docs/db/destructive-delete-proof.md`. The proof
  artifact is created by the Storage Engineering owner before the
  migration is proposed.
- **Mutates a forward migration after merge.** Once a migration is on
  `main`, it is frozen. Corrections are made by adding a new migration
  with a higher number that performs the correcting change.
- **Skips a backend.** A migration that lands for PostgreSQL must have a
  parallel libSQL migration in the same pull request, unless the migration
  is explicitly scoped to a backend-only feature and the scoping is
  documented in the migration header.
- **Bakes in tenant or account identifiers.** Production tenant strings,
  customer identifiers, and similar deployment-shaped values may not
  appear in any file under `db/`.
- **Uses dynamic SQL string concatenation** in any file under
  `db/queries/`. Parameterised queries only.
- **Disables a constraint without a follow-up enable.** Adding a
  `DEFERRABLE` window or a `DISABLE TRIGGER` clause requires a paired
  re-enable migration in the same pull request.

## Required Patterns

- Every migration file begins with the standard Apache-2.0 header
  comment that the rest of the repository uses.
- Every migration declares the backend it targets in its path
  (`db/migrations/postgres/...` vs `db/migrations/libsql/...`).
- Every migration that adds an index also declares whether the index is
  built `CONCURRENTLY` (PostgreSQL) and how the equivalent is achieved
  on libSQL (typically by serialising migrations behind a maintenance
  window).
- Every constraint added under `db/constraints/` cross-references the
  migration that introduced it.

## Negative Tests

The `db-doctor` lane runs the following negative checks and confirms
each one fails with the matching code:

1. A pull request that adds `DROP TABLE consumer_offset;` to a new
   migration without a matching entry in
   `docs/db/destructive-delete-proof.md` must fail with
   `destructive_without_proof`.
2. A pull request that edits the body of an existing forward migration
   on `main` must fail with `forward_migration_mutated`.
3. A pull request that adds a PostgreSQL migration without the parallel
   libSQL migration must fail with `backend_skew`.

## Cross-References

- `docs/db/destructive-delete-proof.md` is the canonical evidence file
  for destructive operations. Every entry there is paired with at least
  one migration here, and every destructive migration here cites at
  least one entry there. The proof file is authored under a sibling
  workstream and is referenced, not duplicated, from this profile.
- `jansu-storage/src/sql/` and `jansu-storage/src/lite/` hold the
  runtime SQL the broker sends; `db/queries/` is the human-facing
  mirror used for review and audit.
- `jansu-storage/src/sql.rs` is the cache loader that materialises the
  runtime SQL strings; review changes there in lockstep with changes to
  `db/queries/`.
- `docs/release/release-readiness.md` includes a launch-gate check that
  every migration shipped in the release candidate has been exercised
  in the staging environment and has a green `db-doctor` lane.

## Rollback Doctrine

A migration that landed on `main` is never rolled back by editing it.
Rollback is performed by adding a new, higher-numbered migration that
performs the inverse operation, accompanied by:

- A proof artifact in `docs/db/destructive-delete-proof.md` if the
  rollback is destructive (it usually is).
- A staging-environment dry run that records the wall-clock duration,
  the row counts touched, and the contention observed.
- A green `db-doctor` lane on the rollback pull request.

## Change Control

Changes to this file require review by a Storage Engineering owner plus
a green `db-doctor` lane. Renames of migration directories must be
reflected in `agent/audit-policy.toml` in the same change set.
