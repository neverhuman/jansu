# Migrations

Install-time SQL lives in `etc/initdb.d/`. Idempotent patch SQL for existing
databases must be tracked by the owning `AUDIT.md` item and verified by a
focused storage test or documented operator procedure.
