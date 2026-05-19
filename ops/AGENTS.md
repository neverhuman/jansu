# Ops Agent Contract

Read the repository root `AGENTS.md` first. This file owns `ops/`,
`.github/workflows/`, `scripts/`, `tools/`, `justfile`, Docker and release
automation.

Allowed work:

- CI workflow hardening and local-parity scripts.
- Security, release, audit, and proof-lane helpers.
- Documentation that explains operational commands and evidence.

Forbidden work:

- Product protocol behavior.
- Storage semantics.
- Compatibility claims not backed by `docs/compatibility/kafka-4.2-ledger.json`.

Proof lane: `just fast`, then the smallest workflow-specific command.
