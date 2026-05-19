# Jankurai Local Standard

This repository uses Jankurai as an audit and repair-routing tool. The
authoritative project contract remains `AGENTS.md`, `MASTER_PLAN.md`,
`AUDIT.md`, `tips/phases/*.md`, and `phase-logs/`.

## Ownership Boundaries

Path ownership is declared in `agent/owner-map.json`. Proof routing is declared
in `agent/test-map.json` and `agent/proof-lanes.toml`.

## Generated Zones

Generated or tool-written files are declared in `agent/generated-zones.toml`.
Do not hand-edit generated protocol surfaces unless the owning phase explicitly
requires it and the regeneration path is verified.

## Repair Receipts

Every repair attempt records the exact command, result, residual risk, and next
recommended action in `phase-logs/attempts/`.
