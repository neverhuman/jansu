# Exceptions

Exceptions must be temporary, owner-assigned, and tied to an audit item or phase
log. Do not use this directory to bypass compatibility proof or hide product
findings.

## Typed Repair Surface

When a crate needs an agent-friendly exception, the record should carry:

- `purpose`: what the exception is for.
- `reason`: why the exception exists now.
- `common_fixes`: the first few concrete ways to clear it.
- `docs_url`: the local doc or phase-log URL for reruns.
- `repair_hint`: the short command or code path the next agent should try.
- `owner`: the crate or team that owns the exception.
- `expires`: the date or phase log after which the exception must be removed.

Keep the record small, actionable, and linked to a real audit item instead of a
free-form debug note.
