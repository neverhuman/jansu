# Boundaries

This document is the agent-readable entry point for repository boundary rules.
The canonical machine-readable boundary map is `agent/boundaries.toml`.

## Runtime Boundaries

- Kafka protocol compatibility is owned by `jansu-sans-io`, `jansu-model`,
  `jansu-broker`, and the compatibility ledger under `docs/compatibility/`.
- The protocol-model exception for Kafka vocabulary in `jansu-model` is
  tracked in `docs/streaming.md` and `agent/boundaries.toml` while AUDIT-019 is
  open; runtime clients still belong behind the adapter boundary.
- Storage behavior is owned by `jansu-storage`, `etc/initdb.d/`, and database
  proof artifacts under `docs/db/`.
- Authentication and authorization behavior is owned by `jansu-auth`,
  `jansu-broker`, and security proof artifacts under `docs/security/`.

## Proof Boundaries

- Compatibility claims require ledger-backed proof in
  `docs/compatibility/kafka-4.2-ledger.json`.
- Audit and score recovery work is tracked by `AUDIT.md` and the Jankurai
  artifacts under `agent/`.
- Generated or audit-written files are listed in `agent/generated-zones.toml`.
