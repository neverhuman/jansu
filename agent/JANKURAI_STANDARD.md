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

## No Masking Of Authored Rust Code

Authored Rust source files (`*.rs`) MUST NOT be listed in
`agent/audit-policy.toml [scan].excluded_paths` nor in
`agent/generated-zones.toml`. The only allowed `.rs` zone entry is `build.rs`
(authored build scripts, where the OUTPUT — not the script — is the generated
artifact). Hiding authored product code from the audit artificially inflates
the Jankurai score and was the root cause of the AUDIT-019 regression
(commit `8a303a4`, 2026-05-16, which masked eight `.rs` files to reach
score 85). The mechanical guard at `tools/checks/no-mask.sh` enforces this
rule and is invoked from both `tools/checks/jankurai-gate.sh` and the
`ops/ci/lib.sh::ci_fast` lane, so masking attempts fail fast locally and in
CI. Bypassing the rule requires editing the guard itself, which is
reviewable.

When the gate fails on a finding, fix it with real work (idiom collapse,
module split, missing test-map route) or accept it as a documented residual
in `AUDIT.md` and a repair receipt under `phase-logs/attempts/`.

## Repair Receipts

Every repair attempt records the exact command, result, residual risk, and next
recommended action in `phase-logs/attempts/`.
