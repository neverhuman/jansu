# MASTER_PLAN

`MASTER_PLAN.md` is the canonical target for prompts that ask for progress on the master plan. Agents must read `AGENTS.md`, this file, `AUDIT.md`, `phase-logs/index.json`, and the relevant phase document before editing code.

## Execution Rule

Pick the highest-priority unblocked item below unless the prompt names a specific phase. Implement only phase-backed work or explicitly cross-phase audit work. Verification and logging are part of done.

Done means:

- Focused tests or checks for the changed behavior pass.
- Broader relevant checks are run or a blocker is recorded.
- The attempt log under `phase-logs/attempts/` is updated.
- The canonical phase log under `phase-logs/` is updated.
- `phase-logs/index.json` still points to the current phase docs, canonical logs, and attempt directories.
- `AUDIT.md` is updated for related open, blocked, or resolved items.
- Compatibility changes point to `docs/compatibility/kafka-4.2-ledger.json` and test proof.

## Priority Queue

Default next **feature** phase after ongoing cross-phase hygiene: **Phase 08** (Phases 04 differential lab, 07 produce exactness, and 10 consumer groups are closed; `AUDIT-004` and `phase-logs/index.json` keep Phase 08 as the active Fetch/ListOffsets/leader-epoch track).

1. `cross-phase` agent context enforcement: keep `AGENTS.md`, `MASTER_PLAN.md`, `AUDIT.md`, tool pointers, phase logs, and compatibility-contract tests aligned.
2. Phase 08: `tips/phases/08-fetch-list-offsets-leader-epoch.md`.
3. Phase 09: `tips/phases/09-topic-config-admin.md`.
4. Phase 11: `tips/phases/11-idempotent-producer.md`.
5. Phase 12: `tips/phases/12-transactions-eos.md`.
6. Phase 13: `tips/phases/13-retention-compaction-delete-records.md`.
7. Phase 14: `tips/phases/14-security-acls-quotas.md`.
8. Phase 15: `tips/phases/15-cluster-metadata-modern-kafka.md`.
9. Phase 16: `tips/phases/16-ecosystem-performance-ops-migration.md`.
10. Phase 17: `tips/phases/17-redlinedb-storage-migration.md`.

Completed or historical phases remain authoritative inputs and may be reopened only for audit-backed fixes.

- **Phase 10** (`tips/phases/10-consumer-groups-offsets.md`): milestone closed 2026-05-03; hardened 2026-05-03 (DynoStore delete_groups fix, failure-mode tests, cooperative-sticky rebalance proof, ledger `failure_modes: partial` for api_keys 8–16, PG migration SQL); canonical log `phase-logs/10-consumer-groups-offsets.md.log`, manifest status `legacy-complete`. Residual: PG migration on pre-existing databases, client-level differential proof.
- **Phase 07** (`tips/phases/07-produce-exactness.md`): completed 2026-05-04. Produce advertised v0..=11 with full validation, snappy encode, frame-size hardening, batch-size validation, 16 storage tests, 5 broker tests. Differential proof infrastructure handed to Phase 04.
- **Phase 04** (`tips/phases/04-differential-kafka-lab.md`): completed 2026-05-04. Kafka 4.2 reference harness, differential_lab.rs (ApiVersions/Metadata/Produce + Phase 08 ListOffsets/Fetch evidence tests), CLI fixtures, CI artifact upload, justfile recipes, ledger proofs for API keys 0, 1, 2, 3, 18, completion guard. AUDIT-012 resolved.

## Phase Order

- Phase 01: `tips/phases/01-compatibility-contract.md`
- Phase 02: `tips/phases/02-api-versions-truth.md`
- Phase 03: `tips/phases/03-request-lifecycle-no-hangs.md`
- Phase 04: `tips/phases/04-differential-kafka-lab.md`
- Phase 05: `tips/phases/05-route-coverage-safe-errors.md`
- Phase 06: `tips/phases/06-storage-log-contract.md`
- Phase 07: `tips/phases/07-produce-exactness.md`
- Phase 08: `tips/phases/08-fetch-list-offsets-leader-epoch.md`
- Phase 09: `tips/phases/09-topic-config-admin.md`
- Phase 10: `tips/phases/10-consumer-groups-offsets.md`
- Phase 11: `tips/phases/11-idempotent-producer.md`
- Phase 12: `tips/phases/12-transactions-eos.md`
- Phase 13: `tips/phases/13-retention-compaction-delete-records.md`
- Phase 14: `tips/phases/14-security-acls-quotas.md`
- Phase 15: `tips/phases/15-cluster-metadata-modern-kafka.md`
- Phase 16: `tips/phases/16-ecosystem-performance-ops-migration.md`
- Phase 17: `tips/phases/17-redlinedb-storage-migration.md`

## Parallel Work

Serial phases:

- Phase 01 defines the contract.
- Phase 02 controls advertisement truth.
- Cross-phase agent context and audit work must coordinate shared manifests.

Parallel-safe phases require disjoint file ownership and separate target directories:

- Phase 08 may run with Phase 09 if storage and broker files do not overlap.
- Phase 10 may run with Phase 14 if coordinator and auth/ACL files do not overlap.
- Phase 11 may run after Phase 07 work is stable.
- Phase 12 depends on Phase 11 semantics and should not run in parallel with Phase 11.
- Phase 13 may run after Phase 06 storage contracts are stable.
- Phase 15 may run with Phase 16 only for documentation or harness work that does not change protocol routing.

Every parallel agent must use a separate `phase-logs/attempts/<phase>/YYYYMMDD-HHMMSS-agent.md` log and a separate `CARGO_TARGET_DIR=/tmp/jansu-verify-<agent-or-phase>`.

## Canonical References

- Agent contract: `AGENTS.md`
- Audit backlog: `AUDIT.md`
- Phase log spec: `phase-logs/README.md`
- Phase log manifest: `phase-logs/index.json`
- Compatibility ledger: `docs/compatibility/kafka-4.2-ledger.json`
- Compatibility contract tests: `jansu-broker/tests/compatibility_contract.rs`
