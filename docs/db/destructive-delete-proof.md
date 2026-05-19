# Destructive SQL Proof

This document tracks SQL files that delete or rewrite rows. It is evidence for
the `db-doctor` lane and a routing document for future Phase 06, 10, 12, and 13
storage work. It does not by itself certify new behavior.

## Rules

- Destructive SQL must be scoped by bound parameters or by a bounded CTE.
- Full-table rewrites require an audit item and focused storage proof.
- Topic, group, credential, transaction, and retention deletes must run through
  storage APIs, not direct callers in broker or CLI code.

## Current Proof Shape

- Postgres SQL is executed through prepared statements in `jansu-storage/src/pg.rs`.
- libSQL SQL is executed through the lite storage backend and query assets under
  `jansu-storage/src/lite/`.
- Topic deletion SQL is scoped by cluster and topic inputs.
- Consumer-group deletion SQL is scoped by cluster and group inputs.
- Retention policy SQL uses CTEs to identify rows eligible for deletion before
  deleting records.

## Required Follow-up

The Jankurai SQL scanner still flags destructive SQL assets for phase-owned
proof. Do not silence those findings with broad policy exclusions. Close them
by adding focused tests or migration receipts in the storage phase that owns the
affected behavior.
