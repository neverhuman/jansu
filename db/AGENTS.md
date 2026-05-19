# DB Agent Contract

Read the repository root `AGENTS.md` first. The durable database surfaces are
`etc/initdb.d/`, `jansu-storage/src/sql/`, and `jansu-storage/src/lite/`.

Allowed work:

- SQL proof documents.
- Migration and schema documentation.
- Focused storage tests tied to a phase or audit item.

Forbidden work:

- Broad storage behavior changes without phase ownership and tests.
- Destructive SQL changes without an explicit proof entry in
  `docs/db/destructive-delete-proof.md`.

Proof lane: the focused `jansu-storage` test named by the owning phase, then
`just db-doctor` for documentation-only changes.
