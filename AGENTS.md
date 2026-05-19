# Agent Contract

This file is the shared entrypoint for Codex, Cursor, Antigravity/Gemini, Claude, Copilot, and any other agent working in this repository.

## Start Here

When a prompt says "progress on MASTER_PLAN and audit", do exactly this:

1. Read `AGENTS.md`.
2. Read `MASTER_PLAN.md`.
3. Read `AUDIT.md`.
4. Read `phase-logs/index.json`.
5. If the prompt names a phase, use that phase. Otherwise pick the highest-priority unblocked item from `MASTER_PLAN.md`.
6. Check `AUDIT.md` for related open or in-progress audit items.
7. Read the relevant `tips/phases/NN-*.md` file before editing code.
8. Create or update an attempt log under `phase-logs/attempts/<phase-or-cross-phase>/`.
9. Implement only work traceable to a phase document or audit item.
10. Run focused verification first, then broader checks.
11. Update the attempt log, canonical phase log, `AUDIT.md`, and `phase-logs/index.json`.
12. Leave `tips/phases/*.md` at least as detailed as before; only append or clarify.

## Canonical Files

- `MASTER_PLAN.md` is the single agent-facing execution map.
- `AUDIT.md` is the canonical audit backlog.
- `tips/phases/*.md` are the phase specifications.
- `phase-logs/index.json` is the phase-log manifest.
- `phase-logs/README.md` defines required log formats.
- `docs/compatibility/kafka-4.2-ledger.json` is the compatibility proof ledger.

No compatibility claim is valid unless the ledger points to proof from tests, differential checks, or client checks.

## Audit Entry Points

- `just score`, `just audit`, and `just jankurai-gate` dispatch through `scripts/ci-local.sh` to the same `tools/checks/jankurai-gate.sh` command used by `.github/workflows/jankurai.yml`.
- The gate writes `agent/repo-score.json` and `agent/repo-score.md`, compares them with `agent/jankurai-gate-baseline.json`, and fails unless both `caps_applied` and `findings` are empty.

## Scope Rules

- Phase docs may be expanded or clarified, but not reduced.
- Work must be phase-backed. Cross-phase work must cite an `AUDIT.md` item marked `cross-phase`.
- Do not implement broad Kafka features while doing setup or bookkeeping work.
- Do not advertise a Kafka API version without ledger and test proof.
- Preserve existing user or agent changes unless the user explicitly asks for a revert.

## Parallel Agents

Multiple agents may work only on disjoint phases or disjoint file ownership. Each agent must use:

- A separate attempt log.
- A separate `CARGO_TARGET_DIR=/tmp/jansu-verify-<agent-or-phase>`.
- Parallel-safe verification commands.

One coordinator must merge shared files such as `phase-logs/index.json`, `AUDIT.md`, and canonical phase logs.

## Logging

Every phase has a canonical log at `phase-logs/NN-slug.md.log` and an attempt directory at `phase-logs/attempts/NN-slug/`.

Every attempt log must include:

- `Agent`
- `Prompt`
- `Phase Or Audit Item`
- `Files Read`
- `Files Changed`
- `Tests Added`
- `Verification Commands`
- `Outcome`
- `Residual Risks`
- `Next Recommended Action`
