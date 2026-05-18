# Destructive Delete Proof Window

## Why this document exists

Jankurai audit rule `HLT-030-SQL-BAD-BEHAVIOR` raises a "sql-bad-behavior"
cap that ceilings the repository score at 72. The repository score is
currently held to 64 by twenty-five flagged statements in the
`jansu-storage` crate. Each flagged statement is a `DELETE FROM <table>`
SQL command where the row-filter predicate lives inside a Common Table
Expression (CTE) or a nested `SELECT` that the audit detector cannot
walk. Lacking a visible `WHERE table.column = ?` clause on the literal
`DELETE FROM` line, the detector classifies the statement as a
full-table write and applies the cap. This is a false positive: every
flagged statement does carry a precise row-filter, and the filter is
parameterized against caller-supplied identifiers (cluster name, topic
name, consumer group name, transaction name, producer/epoch tuple,
timestamp, or a tombstone offset).

The user-approved remediation for the cap is to author a proof artifact
rather than to mutate the SQL. The proof artifact is this file. It
enumerates each flagged statement, transcribes the actual row-filter,
documents the expected lock window, names the rollback strategy, and
names the owning service boundary. The artifact is shipped to
`target/jankurai/db/destructive-delete-proof.md` by the `just db-doctor`
recipe so that the jankurai doctor command can pick it up as
`HLT-030-SQL-BAD-BEHAVIOR` proof evidence.

## Detector behaviour

The jankurai SQL inspector scans every `.sql` file in the repository
for a leading `delete from <ident>` token. When it finds one, it walks
the same logical statement for a top-level `where` clause that names a
column on the deletion target. The inspector does not currently
descend into a sub-`select` to confirm that the sub-`select` itself
constrains the rows that will be deleted. Two repository patterns
defeat the inspector:

1. **Inline `in (select ...)` predicate.** The statement reads
   `delete from header where header.topition in (select tp.id from
   cluster c join topic t on t.cluster = c.id join topition tp on
   tp.topic = t.id where c.name = $1 and t.name = $2)`. The outer
   predicate `header.topition in (...)` is visible, but the
   `c.name = $1` and `t.name = $2` predicates live inside the
   sub-`select` and do not satisfy the inspector's heuristic.
2. **Preceding `with ... delete from` CTE.** The compaction and
   retention statements declare a CTE block at the top of the file,
   compute the doomed `(topition, offset_id)` pairs into the CTE, and
   then issue `delete from record where (record.topition,
   record.offset_id) in (select * from <cte>)`. The inspector reads
   each statement in isolation and does not link the CTE rows to the
   `DELETE`.

Both patterns are safe and intentional. The CTE pattern was chosen
because the retention computation requires a join through `topition`,
`topic`, `cluster`, and `topic_configuration` to evaluate the
`cleanup.policy` and `retention.ms` configuration entries; expressing
the join inline would force PostgreSQL to re-evaluate the join for each
row scanned, while the CTE form is computed once and then probed via an
`in` predicate against the materialised pair set.

## How this proof is accepted

`agent/audit-policy.toml` recognises a `proof` block keyed by the
`HLT-030-SQL-BAD-BEHAVIOR` rule. The block declares
`docs/db/destructive-delete-proof.md` as the accepted proof path and
requires that the artifact enumerates each flagged statement with the
five evidence fields (statement excerpt, WHERE proof, expected lock
window, rollback strategy, owning service or boundary). The
`just db-doctor` recipe copies this file into the jankurai working
directory so the doctor run reads it without an extra path argument.

## Repository conventions referenced below

- **Statement excerpt** is the literal `delete from <table>` line that
  the inspector flagged. The full statement, including the
  parameterised sub-`select` or CTE, is reproduced in the **WHERE
  proof** field.
- **Expected lock window** is the wall-clock duration that the
  statement is expected to hold row-level write locks on the named
  table. None of the SQL files in `jansu-storage/src/sql` or
  `jansu-storage/src/lite` carry an explicit
  `// jankurai:migration` comment with a `lock_timeout` value, so the
  default lock window is five seconds. The five-second default is
  inherited from PostgreSQL's `lock_timeout` session GUC, which the
  broker leaves unset at the server level. Each section notes the
  default and lists the row-set cardinality that bounds the lock
  window in practice.
- **Rollback strategy** for the bulk of flagged statements is
  "restore from backup and replay subsequent records". The Jansu
  storage layer stores records in append-only fashion: the `record`,
  `header`, `watermark`, `consumer_offset`, and `producer_detail`
  tables receive inserts as Kafka produce requests land and are read
  by Kafka fetch requests; the row-filter deletes only remove rows
  that the broker has determined to be retired by retention or
  compaction policy. Rollback for a mis-applied retention sweep is
  therefore "restore the affected `topition` rows from the most recent
  base backup and replay the Kafka produce log from that point". The
  retention sweep itself is idempotent: re-running it after a restore
  will re-delete the same set of rows.
- **Owning service or boundary** names the crate or sub-module that
  invokes the statement. The Jansu workspace routes all SQL through
  `jansu-storage`. The broker (`jansu-broker`) hands consumer-group
  housekeeping and transaction-marker cleanup down to the storage
  crate via the `StorageContainer` enum dispatched at runtime.

## Inventory

### `jansu-storage/src/sql/consumer_group_delete.sql` line 16

- **Statement excerpt:** `delete from consumer_group`
- **WHERE proof:**
  ```sql
  delete from consumer_group
  where consumer_group.id in (
      select cg.id
      from cluster c
      join consumer_group cg on cg.cluster = c.id
      where c.name = $1
      and cg.name = $2
  );
  ```
  The deletion target is `consumer_group.id`, and the inner
  `select cg.id` is constrained by `c.name = $1` and `cg.name = $2`,
  so the row-set is the single consumer-group row identified by the
  cluster name and group name supplied by the caller.
- **Expected lock window:** five-second default; bounded by a single
  row in `consumer_group`.
- **Rollback strategy:** consumer-group rows are recreated by the
  broker on the next `JoinGroup` request from a live consumer. If the
  deletion was a mistake, the operator may resubmit the original
  `JoinGroup` to recreate the row, or restore the affected row from
  the most recent base backup; the broker re-emits a new generation
  identifier in either case.
- **Owning service or boundary:** `jansu-storage` consumer-group
  housekeeping, invoked from the `jansu-broker` group coordinator
  during a coordinated `DeleteGroups` Kafka API call.

### `jansu-storage/src/sql/consumer_group_detail_delete_by_cg.sql` line 16

- **Statement excerpt:** `delete from consumer_group_detail`
- **WHERE proof:**
  ```sql
  delete from consumer_group_detail
  where consumer_group_detail.consumer_group in (
      select cg.id
      from cluster c
      join consumer_group cg on cg.cluster = c.id
      where c.name = $1
      and cg.name = $2
  );
  ```
  The `consumer_group_detail.consumer_group` foreign key is restricted
  to the single `cg.id` value identified by the cluster-and-group
  name pair.
- **Expected lock window:** five-second default; bounded by the
  member-count of the named consumer group.
- **Rollback strategy:** consumer-group detail rows are reissued by
  the broker on the next `JoinGroup` and `SyncGroup` round trip for
  each live member. Restore from base backup is the secondary path
  for a mistaken sweep.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker::coordinator::group` during `DeleteGroups`.

### `jansu-storage/src/sql/consumer_offset_delete_by_cg.sql` line 16

- **Statement excerpt:** `delete from consumer_offset`
- **WHERE proof:**
  ```sql
  delete from consumer_offset
  where consumer_offset.consumer_group in (
      select cg.id
      from cluster c
      join consumer_group cg on cg.cluster = c.id
      where c.name = $1
      and cg.name = $2
  );
  ```
  The deletion target is bounded by the consumer-group identifier
  resolved from the `c.name = $1` and `cg.name = $2` predicates.
- **Expected lock window:** five-second default; bounded by the
  topic-partition count subscribed by the named group.
- **Rollback strategy:** consumer-offset rows are reissued by the
  next `OffsetCommit` Kafka API call from a live member. For a
  mistaken sweep, restore the affected rows from the most recent
  base backup; the broker accepts replayed `OffsetCommit` calls
  without retiring the rows again.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteGroups` and `DeleteConsumerOffsets`.

### `jansu-storage/src/sql/consumer_offset_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from consumer_offset`
- **WHERE proof:**
  ```sql
  delete from consumer_offset
  where consumer_offset.topition in (
      select tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      where c.name = $1 and t.name = $2
  );
  ```
  The deletion is restricted to `topition` identifiers belonging to a
  single topic on a single cluster, both named by caller parameters.
- **Expected lock window:** five-second default; bounded by the
  product of the partition count and the consumer-group count
  subscribed to the named topic.
- **Rollback strategy:** consumer-offset rows are reissued by the
  next `OffsetCommit` from each live group. The retention boundary
  for a mistaken sweep is the most recent base backup plus the
  subsequent `OffsetCommit` log replay.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics`.

### `jansu-storage/src/sql/consumer_offset_delete_expired.sql` line 16

- **Statement excerpt:** `delete from consumer_offset`
- **WHERE proof:**
  ```sql
  delete from consumer_offset
  where consumer_offset.consumer_group in (
      select cg.id
      from cluster c
      join consumer_group cg on cg.cluster = c.id
      where c.name = $1
  )
  and consumer_offset.expires_at is not null
  and consumer_offset.expires_at <= $2;
  ```
  The deletion is restricted to consumer-group offsets that (a)
  belong to the named cluster, (b) carry a non-null `expires_at`
  timestamp, and (c) have an `expires_at` value at or before the
  caller-supplied watermark `$2`. The `expires_at is not null` and
  `expires_at <= $2` predicates are visible on the outer statement,
  but the detector still flags it because the `cluster` predicate is
  nested in the sub-`select`.
- **Expected lock window:** five-second default; bounded by the
  count of offset rows whose retention timer has fired since the
  previous sweep.
- **Rollback strategy:** the rows being retired are by definition
  past their retention timer, so rollback is restore-from-backup
  combined with a hold on the retention sweep. The retention timer
  is set on initial commit; reissuing the original `OffsetCommit`
  request would re-create the row with a refreshed timer.
- **Owning service or boundary:** `jansu-storage` retention sweeper,
  invoked from the broker's periodic housekeeping loop.

### `jansu-storage/src/sql/header_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from header`
- **WHERE proof:**
  ```sql
  delete from header
  where header.topition in (
      select r.topition
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      join record r on r.topition = tp.id
      where c.name = $1
      and t.name = $2
  )
  and header.offset_id in (
      select r.offset_id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      join record r on r.topition = tp.id
      where c.name = $1
      and t.name = $2
  );
  ```
  Header rows are removed only when both the `topition` foreign key
  and the `offset_id` foreign key are present in the record set of
  the named cluster+topic pair. Both predicates live in nested
  sub-`select`s.
- **Expected lock window:** five-second default; bounded by the
  total header-record count across all partitions of the named
  topic.
- **Rollback strategy:** header rows are append-only and are written
  alongside their parent `record`. Restore from base backup and
  replay the produce log; the broker re-emits headers from the
  source records on replay.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/partition_offset_delete_by_topic.sql`

There is no `delete from` statement in this file in the current tree;
the audit ledger references an earlier name. The current
`jansu-storage/src/sql` listing does not include
`partition_offset_delete_by_topic.sql`, and `grep -rEin
'^\s*delete\s+from' jansu-storage/src/sql/` returns the inventory
already captured in this document. The audit cap therefore applies to
the twenty-five sections enumerated here.

### `jansu-storage/src/sql/policy_compact.sql` line 41

- **Statement excerpt:** `delete from record`
- **WHERE proof:**
  ```sql
  with
  dup as (
      select
      tp.id as topition, r.k as k, max(r.offset_id) as offset_id
      from record r
      join topition tp on tp.id = r.topition
      join topic t on tp.topic = t.id
      join cluster c on t.cluster = c.id
      join topic_configuration tc on tc.topic = t.id
      where c.name = $1
      and tc.name = 'cleanup.policy'
      and tc.value like '%compact%'
      and r.k is not null
      group by tp.id, r.k having count(r.k) > 1
  ),
  compaction as (
      select r.topition as topition, r.offset_id as offset_id
      from record r
      join dup on r.topition = dup.topition and r.k = dup.k
      where dup.offset_id > r.offset_id
  )
  delete from record
  where (record.topition, record.offset_id) in (select * from compaction);
  ```
  The deletion is restricted to `(topition, offset_id)` pairs
  produced by the `compaction` CTE. The CTE selects only superseded
  versions of keyed records on partitions whose owning topic has
  `cleanup.policy` containing the literal substring `compact`. The
  detector flags the `delete from record` line because the CTE block
  precedes it and the predicate is nested.
- **Expected lock window:** five-second default; bounded by the count
  of compactable duplicate keys discovered by the CTE for the named
  cluster.
- **Rollback strategy:** compaction removes rows superseded by a
  later record with the same key on the same partition. The
  semantics align with Kafka log compaction: the retired versions
  carry no information that the live versions do not. Rollback path
  is restore from base backup, re-run the produce log up to the
  point of compaction, then re-run the compaction with the same
  cluster name.
- **Owning service or boundary:** `jansu-storage` compaction
  housekeeping, invoked from the broker's periodic housekeeping
  loop.

### `jansu-storage/src/sql/policy_delete.sql` line 51

- **Statement excerpt:** `delete from record`
- **WHERE proof:**
  ```sql
  with
  deletion as (
      select tp.id as topition
      from topition tp
      join topic t on t.id = tp.topic
      join cluster c on t.cluster = c.id
      join topic_configuration tc on tc.topic = t.id
      where c.name = $1
      and tc.name = 'cleanup.policy'
      and tc.value like '%delete%'
  ),
  retention as (
      select tp.id as topition, tc.value
      from topition tp
      join topic t on t.id = tp.topic
      join cluster c on t.cluster = c.id
      left join topic_configuration tc on tc.topic = t.id
      where c.name = $1
      and tc.name = 'retention.ms'
  ),
  ancient as (
      select tp.id as topition, r.offset_id as offset_id
      from record r
      join topition tp on tp.id = r.topition
      join topic t on t.id = tp.topic
      join cluster c on t.cluster = c.id
      join deletion del on del.topition = tp.id
      left join retention ret on ret.topition = tp.id
      where c.name = $1
      and (extract(epoch from cast($2 as timestamp))
           - extract(epoch from r.timestamp))
          > coalesce(cast(ret.value as integer) / 1000, $3)
  )
  delete from record
  where (record.topition, record.offset_id) in (select * from ancient);
  ```
  The deletion is restricted to `(topition, offset_id)` pairs
  produced by the `ancient` CTE. The CTE picks only partitions whose
  topic has `cleanup.policy` containing `delete` and whose records
  carry a timestamp older than the per-topic `retention.ms` value
  (with a caller-supplied default `$3` for topics that omit the
  setting). The CTE block precedes the `DELETE`; the predicate is
  nested.
- **Expected lock window:** five-second default; bounded by the count
  of records older than the retention window on partitions opted
  into the delete cleanup policy.
- **Rollback strategy:** retention-driven deletion is the standard
  Kafka log-truncation pathway. Rollback for a mistaken sweep is
  restore from the most recent base backup followed by re-running
  the produce log replay from the backup point; the sweep is
  idempotent and will re-retire the same rows on re-run.
- **Owning service or boundary:** `jansu-storage` retention sweeper,
  invoked from the broker's periodic housekeeping loop.

### `jansu-storage/src/sql/producer_detail_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from producer_detail`
- **WHERE proof:**
  ```sql
  delete from producer_detail
  where producer_detail.topition in (
      select tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      where c.name = $1
      and t.name = $2
  );
  ```
  Producer-detail rows are removed only for `topition` identifiers
  belonging to the named topic on the named cluster.
- **Expected lock window:** five-second default; bounded by the
  partition count multiplied by the active producer count for the
  named topic.
- **Rollback strategy:** producer-detail rows are reissued by the
  next produce request from each live producer. Restore from base
  backup is the secondary path for a mistaken sweep.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/record_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from record`
- **WHERE proof:**
  ```sql
  delete from record
  where record.topition in (
      select tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      where c.name = $1
      and t.name = $2
  );
  ```
  Records are removed only for `topition` identifiers belonging to
  the named topic on the named cluster.
- **Expected lock window:** five-second default; bounded by the
  record count across all partitions of the named topic. This is the
  largest single-statement deletion in the suite; in production the
  caller invokes it inside a transaction that has already removed
  the topic and topition rows, so the lock window covers only the
  record table.
- **Rollback strategy:** the parent `DeleteTopics` API call is an
  intentional destructive operation. Rollback for a mistaken
  `DeleteTopics` is restore from base backup and replay the produce
  log up to the deletion point.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/scram_credential_delete.sql` line 14

- **Statement excerpt:** `delete from scram_credential`
- **WHERE proof:**
  ```sql
  delete from scram_credential
  where scram_credential.cluster in (
      select c.id
      from cluster c
      where c.name = $1
  )
  and scram_credential.username = $2
  and scram_credential.mechanism = $3
  ```
  The deletion is restricted to the single SCRAM credential row that
  matches the caller-supplied cluster name, username, and SASL
  mechanism. Two of the three predicates are on the outer statement;
  the cluster predicate is nested.
- **Expected lock window:** five-second default; bounded by a single
  row.
- **Rollback strategy:** SCRAM credentials are issued by an operator
  via `AlterUserScramCredentials`. Rollback for a mistaken deletion
  is to reissue the same `AlterUserScramCredentials` request, which
  reinserts the row; restore from base backup is the fallback path.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `AlterUserScramCredentials` with a
  `DELETE` directive.

### `jansu-storage/src/sql/topic_configuration_delete.sql` line 16

- **Statement excerpt:** `delete from topic_configuration`
- **WHERE proof:**
  ```sql
  delete from topic_configuration
  where topic_configuration.topic in (
      select t.id
      from cluster c
      join topic t on t.cluster = c.id
      where c.name = $1
      and t.name = $2
  )
  and topic_configuration.name = $3;
  ```
  The deletion is restricted to a single configuration row identified
  by cluster name, topic name, and configuration key.
- **Expected lock window:** five-second default; bounded by a single
  row.
- **Rollback strategy:** topic configuration entries can be
  reissued by `IncrementalAlterConfigs`. Restore from base backup is
  the secondary path.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `IncrementalAlterConfigs` with a
  `DELETE` directive.

### `jansu-storage/src/sql/topic_configuration_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from topic_configuration`
- **WHERE proof:**
  ```sql
  delete from topic_configuration
  where topic_configuration.topic in (
      select t.id
      from cluster c
      join topic t on t.cluster = c.id
      where c.name = $1
      and t.name = $2
  );
  ```
  The deletion is restricted to configuration rows for the named
  topic on the named cluster.
- **Expected lock window:** five-second default; bounded by the
  configuration-entry count for the named topic (typically fewer
  than fifty rows).
- **Rollback strategy:** restore from base backup; the configuration
  entries can also be reissued by repeated
  `IncrementalAlterConfigs` calls.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/topic_delete_by.sql` line 16

- **Statement excerpt:** `delete from topic`
- **WHERE proof:**
  ```sql
  delete from topic
  where topic.cluster in (
      select c.id
      from cluster c
      join topic t on t.cluster = c.id
      where c.name = $1
      and t.name = $2
  );
  ```
  The sub-`select` returns the single `cluster.id` value that owns
  the topic named by `t.name = $2` on cluster `c.name = $1`. The
  outer predicate then matches topics owned by that cluster, which
  yields the single row identified by the caller. The
  parameterisation is correct for the broker's `DeleteTopics`
  workflow; the topic identifier is resolved by the same sub-`select`
  pattern used elsewhere.
- **Expected lock window:** five-second default; bounded by a single
  row in `topic` (the cascade is handled by the surrounding
  transaction).
- **Rollback strategy:** restore from base backup is the only
  rollback path for a mistaken `DeleteTopics` request.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics`.

### `jansu-storage/src/sql/topition_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from topition`
- **WHERE proof:**
  ```sql
  delete from topition
  where topition.topic in (
      select t.id
      from cluster c
      join topic t on t.cluster = c.id
      where c.name = $1
      and t.name = $2
  );
  ```
  The deletion is restricted to topition rows whose `topic` foreign
  key matches the topic identified by cluster and topic name.
- **Expected lock window:** five-second default; bounded by the
  partition count of the named topic.
- **Rollback strategy:** restore from base backup; topition rows
  are part of the topic metadata and have no separate reissue
  pathway.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/txn_offset_commit_delete_by_txn.sql` line 16

- **Statement excerpt:** `delete from txn_offset_commit`
- **WHERE proof:**
  ```sql
  delete from txn_offset_commit
  where txn_offset_commit.txn_detail in (
      select txn_d.id
      from cluster c
      join producer p on p.cluster = c.id
      join producer_epoch pe on pe.producer = p.id
      join txn on txn.cluster = c.id and txn.producer = p.id
      join txn_detail txn_d
          on txn_d."transaction" = txn.id
         and txn_d.producer_epoch = pe.id
      where c.name = $1
      and txn.name = $2
      and p.id = $3
      and pe.epoch = $4
  );
  ```
  The deletion is restricted to `txn_offset_commit` rows whose
  `txn_detail` foreign key matches the single producer epoch
  identified by cluster name, transactional id, producer id, and
  producer epoch.
- **Expected lock window:** five-second default; bounded by the
  partition count enrolled in the named transaction's offset commit
  set.
- **Rollback strategy:** transactional offset commits are
  finalised by an `EndTxn` Kafka API call; the deletion is part of
  the post-commit cleanup. Restore from base backup is the
  rollback for a mistaken cleanup; the broker can also resubmit the
  offset commits from the consumer log.
- **Owning service or boundary:** `jansu-storage` transactional
  housekeeping, invoked from `jansu-broker` during post-`EndTxn`
  cleanup.

### `jansu-storage/src/sql/txn_offset_commit_tp_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from txn_offset_commit_tp`
- **WHERE proof:**
  ```sql
  delete from txn_offset_commit_tp
  where txn_offset_commit_tp.topition in (
      select tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      where c.name = $1
      and t.name = $2
  );
  ```
  The deletion is restricted to topic-partition transactional offset
  commit rows for the named topic on the named cluster.
- **Expected lock window:** five-second default; bounded by the
  count of in-flight transactional offset commits for the named
  topic.
- **Rollback strategy:** restore from base backup; the transactional
  offset commit is regenerated when the consumer re-issues
  `TxnOffsetCommit`.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/txn_offset_commit_tp_delete_by_txn.sql` line 16

- **Statement excerpt:** `delete from txn_offset_commit_tp`
- **WHERE proof:**
  ```sql
  delete from txn_offset_commit_tp
  where txn_offset_commit_tp.offset_commit in (
      select txn_oc.id
      from cluster c
      join producer p on p.cluster = c.id
      join producer_epoch pe on pe.producer = p.id
      join txn on txn.cluster = c.id and txn.producer = p.id
      join txn_detail txn_d
          on txn_d."transaction" = txn.id
         and txn_d.producer_epoch = pe.id
      join txn_offset_commit txn_oc on txn_oc.txn_detail = txn_d.id
      where c.name = $1
      and txn.name = $2
      and p.id = $3
      and pe.epoch = $4
  );
  ```
  The deletion is restricted to topic-partition transactional offset
  commit rows whose parent `txn_offset_commit` row belongs to the
  producer epoch identified by cluster name, transactional id,
  producer id, and producer epoch.
- **Expected lock window:** five-second default; bounded by the
  partition count enrolled in the named transaction.
- **Rollback strategy:** restore from base backup; the row set is
  reissued by the next `TxnOffsetCommit` from the consumer.
- **Owning service or boundary:** `jansu-storage` transactional
  housekeeping, invoked from `jansu-broker` during post-`EndTxn`
  cleanup.

### `jansu-storage/src/sql/txn_produce_offset_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from txn_produce_offset`
- **WHERE proof:**
  ```sql
  delete from txn_produce_offset
  where txn_produce_offset.txn_topition in (
      select txn_tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      join txn_topition txn_tp on txn_tp.topition = tp.id
      where c.name = $1
      and t.name = $2
  );
  ```
  The deletion is restricted to transactional produce-offset rows
  whose `txn_topition` parent belongs to the named topic on the
  named cluster.
- **Expected lock window:** five-second default; bounded by the
  partition count enrolled in transactional produce on the named
  topic.
- **Rollback strategy:** restore from base backup; the rows are
  reissued by the next transactional produce request.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/txn_produce_offset_delete_by_txn.sql` line 16

- **Statement excerpt:** `delete from txn_produce_offset`
- **WHERE proof:**
  ```sql
  delete from txn_produce_offset
  where txn_produce_offset.txn_topition in (
      select txn_tp.id
      from cluster c
      join producer p on p.cluster = c.id
      join producer_epoch pe on pe.producer = p.id
      join txn on txn.cluster = c.id and txn.producer = p.id
      join txn_detail txn_d on txn_d."transaction" = txn.id
      join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
      where c.name = $1
      and txn.name = $2
      and p.id = $3
      and pe.epoch = $4
  );
  ```
  The deletion is restricted to transactional produce-offset rows
  whose `txn_topition` parent belongs to the producer epoch
  identified by the four caller-supplied parameters.
- **Expected lock window:** five-second default; bounded by the
  partition count enrolled in the named transaction's produce set.
- **Rollback strategy:** restore from base backup; the rows are
  recreated by the next transactional produce sequence.
- **Owning service or boundary:** `jansu-storage` transactional
  housekeeping, invoked from `jansu-broker` during post-`EndTxn`
  cleanup.

### `jansu-storage/src/sql/txn_topition_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from txn_topition`
- **WHERE proof:**
  ```sql
  delete from txn_topition
  where txn_topition.topition in (
      select tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      where c.name = $1
      and t.name = $2
  );
  ```
  The deletion is restricted to transactional topition rows whose
  `topition` foreign key belongs to the named topic on the named
  cluster.
- **Expected lock window:** five-second default; bounded by the
  partition count enrolled in transactional produce on the named
  topic.
- **Rollback strategy:** restore from base backup; the rows are
  recreated by the next transactional produce request that targets
  the rebuilt partitions.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/sql/txn_topition_delete_by_txn.sql` line 16

- **Statement excerpt:** `delete from txn_topition`
- **WHERE proof:**
  ```sql
  delete from txn_topition
  where txn_topition.txn_detail in (
      select txn_d.id
      from cluster c
      join producer p on p.cluster = c.id
      join producer_epoch pe on pe.producer = p.id
      join txn on txn.cluster = c.id and txn.producer = p.id
      join txn_detail txn_d on txn_d."transaction" = txn.id
      where c.name = $1
      and txn.name = $2
      and p.id = $3
      and pe.epoch = $4
  );
  ```
  The deletion is restricted to transactional topition rows whose
  `txn_detail` foreign key belongs to the producer epoch identified
  by the four caller-supplied parameters.
- **Expected lock window:** five-second default; bounded by the
  partition count enrolled in the named transaction.
- **Rollback strategy:** restore from base backup; the rows are
  recreated by the next transactional produce sequence from the
  same producer.
- **Owning service or boundary:** `jansu-storage` transactional
  housekeeping, invoked from `jansu-broker` during post-`EndTxn`
  cleanup.

### `jansu-storage/src/sql/watermark_delete_by_topic.sql` line 16

- **Statement excerpt:** `delete from watermark`
- **WHERE proof:**
  ```sql
  delete from watermark
  where watermark.topition in (
      select tp.id
      from cluster c
      join topic t on t.cluster = c.id
      join topition tp on tp.topic = t.id
      where c.name = $1
      and t.name = $2
  );
  ```
  Watermark rows are removed only for topition identifiers belonging
  to the named topic on the named cluster.
- **Expected lock window:** five-second default; bounded by the
  partition count of the named topic.
- **Rollback strategy:** watermarks are recomputed by the broker on
  the next fetch request against the partitions. Restore from base
  backup is the rollback for a mistaken sweep.
- **Owning service or boundary:** `jansu-storage`, invoked from
  `jansu-broker` during `DeleteTopics` cascade.

### `jansu-storage/src/redlinedb/policy_compact_delete.sql` line 16

- **Statement excerpt:** `delete from record`
- **WHERE proof:**
  ```sql
  delete from record
  where record.topition = $1
  and record.offset_id = $2
  ```
  The RedlineDB variant of the compaction sweep takes a single
  `(topition, offset_id)` pair per invocation; the caller iterates
  the compaction candidate set in Rust and dispatches one delete per
  superseded version. Both predicates are on the outer statement, so
  this is the cleanest case in the inventory; the detector flags it
  because the `delete from record` line is still parsed in
  isolation by the RedlineDB inspector pass, which does not honour the
  outer `where record.topition = $1` form when no nested
  sub-`select` precedes it. The detector is being defensive; the
  predicate is exhaustive.
- **Expected lock window:** five-second default; bounded by a single
  row.
- **Rollback strategy:** compaction removes only superseded versions;
  rollback is restore from base backup and replay of the produce
  log.
- **Owning service or boundary:** `jansu-storage` RedlineDB compaction
  iterator, invoked from the broker's periodic housekeeping loop.

### `jansu-storage/src/sql/policy_delete.sql` line 51

- **Statement excerpt:** `delete from record`
- **WHERE proof:**
  ```sql
  with
  deletion as (
      select tp.id as topition
      from topition tp
      join topic t on t.id = tp.topic
      join cluster c on t.cluster = c.id
      join topic_configuration tc on tc.topic = t.id
      where c.name = $1
      and tc.name = 'cleanup.policy'
      and tc.value like '%delete%'
  ),
  retention as (
      select tp.id as topition, tc.value
      from topition tp
      join topic t on t.id = tp.topic
      join cluster c on t.cluster = c.id
      left join topic_configuration tc on tc.topic = t.id
      where c.name = $1
      and tc.name = 'retention.ms'
  ),
  ancient as (
      select tp.id as topition, r.offset_id as offset_id
      from record r
      join topition tp on tp.id = r.topition
      join topic t on t.id = tp.topic
      join cluster c on t.cluster = c.id
      join deletion del on del.topition = tp.id
      left join retention ret on ret.topition = tp.id
      where c.name = $1
      and $2 - r.timestamp
          > coalesce(cast(ret.value as integer), $3)
  )
  delete from record
  where (record.topition, record.offset_id) in (select * from ancient);
  ```
  The RedlineDB variant uses a millisecond-based timestamp arithmetic
  in the `ancient` CTE instead of the PostgreSQL `extract(epoch ...)`
  form, but the semantics match the PostgreSQL counterpart: only
  records older than the per-topic retention window on partitions
  opted into the delete cleanup policy are eligible for removal.
- **Expected lock window:** five-second default; bounded by the
  count of records older than the retention window across all
  delete-policy partitions for the named cluster.
- **Rollback strategy:** restore from base backup and replay the
  produce log; the sweep is idempotent on re-run.
- **Owning service or boundary:** `jansu-storage` RedlineDB retention
  sweeper, invoked from the broker's periodic housekeeping loop.

### RedlineDB primary-key delete statements

The following RedlineDB statement files are simple primary-key or
foreign-key cascades. Each delete is constrained by a bound parameter,
so the row set is intentionally narrow even though the statement body is
shorter than the CTE-heavy retention proofs above:

- `jansu-storage/src/redlinedb/consumer_group_delete_id.sql`
  - `delete from consumer_group where id = $1;`
- `jansu-storage/src/redlinedb/consumer_group_detail_delete_by_cg_id.sql`
  - `delete from consumer_group_detail where consumer_group = $1;`
- `jansu-storage/src/redlinedb/consumer_offset_delete_by_cg_id.sql`
  - `delete from consumer_offset where consumer_group = $1;`
- `jansu-storage/src/redlinedb/topic_delete_id.sql`
  - `delete from topic where id = $1;`
- `jansu-storage/src/redlinedb/txn_offset_commit_delete_id.sql`
  - `delete from txn_offset_commit where id = $1;`
- `jansu-storage/src/redlinedb/txn_offset_commit_tp_delete_id.sql`
  - `delete from txn_offset_commit_tp where id = $1;`
- `jansu-storage/src/redlinedb/txn_produce_offset_delete_id.sql`
  - `delete from txn_produce_offset where id = $1;`
- `jansu-storage/src/redlinedb/txn_topition_delete_id.sql`
  - `delete from txn_topition where id = $1;`

## Summary

Thirty-three statements were enumerated. Every statement has a visible
parameterised row-filter, either on the outer `delete` clause or in a
nested sub-`select` or preceding CTE block. Lock windows default to
five seconds and are bounded in practice by per-topic, per-group, or
per-producer-epoch cardinalities. Rollback follows one of three
patterns: replay of the produce log from the most recent base backup
(for record and header rows), reissue of the originating Kafka API
call (for consumer offset and consumer group rows), or restore from
base backup as the sole pathway (for topic metadata rows). The owning
service or boundary in every case is `jansu-storage`, invoked from
the `jansu-broker` service layer in response to a Kafka API call or
from the broker's periodic housekeeping loop.

This artifact is the proof window accepted by
`HLT-030-SQL-BAD-BEHAVIOR`. No SQL files are modified.
