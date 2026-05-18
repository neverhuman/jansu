# Authorization Matrix

This document is the canonical principal-by-resource-by-operation matrix
for the Jansu broker. It is derived from the Kafka ACL primitives encoded
in `jansu-sans-io/src/acl.rs` and the resource pattern primitives encoded
in `jansu-sans-io/src/resource.rs`, and it cross-references the negative
authorization tests in
`jansu-broker/src/coordinator/group/administrator/tests.rs`. The matrix
is the single source of truth that other security documents (input
boundary, agent supply chain, release readiness) link back to.

## Sources

- Operations enum: `jansu-sans-io/src/acl.rs:63-82`
- Permissions enum: `jansu-sans-io/src/acl.rs:15-32`
- Resources enum: `jansu-sans-io/src/acl.rs:108-137`
- Pattern types: `jansu-sans-io/src/resource.rs:15-36`
- Operation implication rules: `jansu-sans-io/src/acl.rs:46-60`

## Principals

A principal is the authenticated identity attached to a Kafka connection.
Jansu inherits the Kafka principal model:

- `anonymous` - The connection has not completed a SASL exchange and the
  listener does not require authentication. Anonymous principals exist
  only on listeners explicitly configured to accept them; on a
  production listener, the anonymous row is always empty.
- `user:<name>` - A SASL principal authenticated via PLAIN or SCRAM. The
  name is the username asserted during the handshake and persisted on
  the connection by `jansu-auth/src/handshake.rs`.
- `service:<name>` - A SASL principal that represents an internal service
  account. Service principals are distinguished from human users by a
  name prefix recorded in the cluster's principal mapping.
- `broker:<id>` - The broker's own internal identity, used for
  inter-broker requests. In Jansu's single-node design (node id 111),
  the broker principal is only meaningful when a future version
  introduces a multi-node deployment.

Principal resolution happens once, at SASL completion. After that, the
principal is immutable for the lifetime of the connection.

## Resources

The Kafka resource types Jansu recognises are encoded in
`jansu-sans-io/src/acl.rs:108-137`:

| Resource enum value | Kafka name | Surface in Jansu |
|---|---|---|
| `Topic` (2) | Topic | Persisted message log, including its partitions. |
| `Group` (3) | Consumer group | Group coordinator state under `jansu-broker/src/coordinator/group/`. |
| `Cluster` (4) | Cluster | Cluster-wide control surface (broker registration, controller actions). |
| `TransactionalId` (5) | Transactional id | Producer transaction state. |
| `DelegationToken` (6) | Delegation token | Token-based authentication artefacts. |
| `User` (7) | User principal | SCRAM credential management for a named user. |

`Unknown` (0) and `Any` (1) are sentinel values used in filters and
wire-level decoding; they never appear as a real resource.

## Operations

The Kafka operation types Jansu recognises are encoded in
`jansu-sans-io/src/acl.rs:63-82`:

| Operation | Numeric | Implies (per `acl.rs:46-60`) |
|---|---|---|
| Read | 3 | Describe |
| Write | 4 | Describe |
| Create | 5 | - |
| Delete | 6 | Describe |
| Alter | 7 | Describe |
| Describe | 8 | - |
| ClusterAction | 9 | - |
| DescribeConfigs | 10 | - |
| AlterConfigs | 11 | DescribeConfigs |
| IdempotentWrite | 12 | - |
| CreateTokens | 13 | - |
| DescribeTokens | 14 | - |
| TwoPhaseCommit | 15 | - |
| All | 2 | every operation listed above |

`Unknown` (0) and `Any` (1) are sentinel values used in filters and
wire-level decoding.

## Resource Pattern Types

The resource pattern types are encoded in
`jansu-sans-io/src/resource.rs:15-36`:

- `Literal` (3) - Exact name match. `topic:orders` matches only the
  topic named `orders`.
- `Prefixed` (4) - Prefix match. `topic:orders.` matches `orders.eu`,
  `orders.us`, and so on.
- `Match` (2) - Filter-only pattern for ACL queries.
- `Any` (1) and `Unknown` (0) - Filter or sentinel values.

The matrix below treats pattern resolution as orthogonal to the
operation table: every cell is evaluated against the same precedence
rules regardless of whether the matching ACL was Literal or Prefixed.

## Precedence Rules

Authorization is evaluated in this order:

1. If any matching ACL has `Permission::Deny` for the requested
   operation (or for an operation that implies it through the
   `acl.rs:46-60` table), deny.
2. Otherwise, if any matching ACL has `Permission::Allow` for the
   requested operation (or for an implying operation), allow.
3. Otherwise, deny by default.

`Permission::Unknown` is treated as deny. `Permission::Any` is a
filter-only value and never appears in an effective ACL.

## Matrix

The cells below describe the operations each principal class may
perform against each resource class on a production listener with no
explicit ACL overrides beyond those Jansu installs by default. `A`
means allow, `D` means deny, and `-` means not applicable (the
operation is not defined for that resource).

| Principal class \ Resource | Topic | Group | Cluster | TransactionalId | DelegationToken | User |
|---|---|---|---|---|---|---|
| `anonymous` | D | D | D | D | D | D |
| `user:<name>` (no ACL) | D | D | D | D | D | D |
| `user:<name>` (Read on topic) | Read, Describe | D | D | D | D | D |
| `user:<name>` (Write on topic) | Write, Describe, IdempotentWrite (cluster Allow IdempotentWrite required) | D | D | D | D | D |
| `user:<name>` (Read on group) | D | Read, Describe | D | D | D | D |
| `user:<name>` (Write on transactional id) | Write, Describe (topic ACL required) | D | D | Write, Describe | D | D |
| `service:<name>` (Alter on topic) | Alter, Describe, AlterConfigs, DescribeConfigs | D | D | D | D | D |
| `service:<name>` (ClusterAction on cluster) | D | D | ClusterAction | D | D | D |
| `service:<name>` (All on cluster) | A | A | A | A | A | A |
| `broker:<id>` (internal) | A | A | A | A | A | A |
| Token-creating service (CreateTokens on cluster) | D | D | CreateTokens, Describe | D | A (issuer view) | D |
| User-admin service (Alter on User) | D | D | D | D | D | Alter, Describe |

The `service:<name>` rows are illustrative: a real service principal
holds only the ACLs explicitly granted to it, and the matrix above is
not a substitute for reading the ACL store.

## SASL to Authorization Binding

```
+--------------------+        +--------------------+        +-------------------------+
| TCP connection     | -----> | SaslHandshake      | -----> | SaslAuthenticate        |
| (no principal yet) |        | (jansu-auth)       |        | (jansu-auth)            |
+--------------------+        +--------------------+        +-------------------------+
                                                                          |
                                                                          v
                                                          +------------------------------+
                                                          | Principal attached to        |
                                                          | connection context           |
                                                          +------------------------------+
                                                                          |
                                                                          v
                                                          +------------------------------+
                                                          | Per-request authorization    |
                                                          | check against ACL store      |
                                                          | (uses Operation + Resource   |
                                                          | + Pattern from acl.rs)       |
                                                          +------------------------------+
                                                                          |
                                                                          v
                                                          +------------------------------+
                                                          | Allow -> handler in          |
                                                          | jansu-broker                 |
                                                          | Deny  -> ErrorCode return    |
                                                          +------------------------------+
```

The handshake step at `jansu-auth/src/handshake.rs` chooses the
mechanism (PLAIN or SCRAM). The authenticate step exchanges credentials
and, on success, attaches a principal to the connection. Every
subsequent request flows into a per-request authorization check that
consults the ACL store using the (Operation, Resource, Pattern) tuple
defined in `jansu-sans-io/src/acl.rs` and `jansu-sans-io/src/resource.rs`.

## Negative Test Pointers

The following tests in
`jansu-broker/src/coordinator/group/administrator/tests.rs` exercise
the negative paths of group-coordinator authorization and membership
validation. They are the canonical regression net for the Group rows of
the matrix:

- `heartbeat_from_unknown_member_returns_error` at
  `jansu-broker/src/coordinator/group/administrator/tests.rs:1569` -
  Asserts that a `Heartbeat` from a member id that the coordinator has
  never seen is rejected with the appropriate error code. This is the
  membership-level analogue of an authorization failure: the principal
  may be authenticated, but the membership predicate fails.
- `leave_unknown_member_returns_per_member_error` at
  `jansu-broker/src/coordinator/group/administrator/tests.rs:2655` -
  Asserts that a `LeaveGroup` request that names a member the
  coordinator has no record of returns a per-member error rather than
  succeeding silently. This guards against a class of bug in which a
  forged or expired member id would be accepted.
- `lifecycle` at
  `jansu-broker/src/coordinator/group/administrator/tests.rs:108` -
  The end-to-end positive path that exercises a full join, sync,
  heartbeat, and leave cycle for a single consumer group. It is the
  baseline against which the two negative tests above are interpreted:
  the positive path must remain green for the negative paths to be
  meaningful.

Additional negative coverage for the Topic, Cluster, and
TransactionalId rows lives in the broker integration tests at
`jansu-broker/tests/` and is exercised by the cross-backend test
matrix.

## Broker Isolation {#broker-isolation}

The Jansu broker enforces several isolation boundaries that the
authorization matrix relies on. These are documented here so that other
security documents can link back to the `#broker-isolation` anchor.

1. **Principal isolation.** A principal authenticated on one connection
   has no effect on any other connection. Principals are derived from
   the SASL exchange that ran on that specific socket, and there is no
   shared mutable state through which a principal could leak between
   connections.
2. **Cluster identity isolation.** The cluster id is supplied at broker
   startup (`--kafka-cluster-id`) and is encoded into every ACL match.
   A request that arrives at a broker bound to cluster `A` cannot, by
   construction, match an ACL whose cluster scope is `B`.
3. **Tenant isolation through topic prefixes.** When a deployment uses
   the `Prefixed` resource pattern to carve a namespace per tenant
   (`topic:t1.` for tenant 1, `topic:t2.` for tenant 2), the broker's
   ACL matcher uses byte-level prefix matching on the topic name. There
   is no glob expansion or regex evaluation, which keeps the matcher
   immune to a class of regex-injection attacks.
4. **Storage backend isolation.** Each backend (PostgreSQL, RedlineDB,
   SlateDB, S3) is selected at startup via a single storage URL. The
   broker never holds simultaneous open handles to two production
   backends; the only multi-backend configurations are the test
   matrices under `jansu-broker/tests/`.
5. **Coordinator-state isolation.** The group coordinator state in
   `jansu-broker/src/coordinator/group/` is scoped by `(cluster_id,
   group_id)`. A `Heartbeat`, `JoinGroup`, `SyncGroup`, or `LeaveGroup`
   that arrives with a mismatching cluster id is rejected before the
   member identity is even consulted.
6. **Wire-protocol isolation.** All inbound bytes pass through the
   sans-I/O decoder at `jansu-sans-io/src/de.rs`, which enforces a
   maximum frame size of one gibibyte
   (`MESSAGE_MAX_SIZE` at line 31). This caps the surface that any
   single connection can present to the broker's allocator.

## Audit Hooks

Every authorization decision (allow or deny) is emitted via the
`tracing` instrumentation already present in the broker handlers. The
audit lane captures these events in a structured form so that an
operator can replay a request's authorization trail without having to
reconstruct it from raw logs. The audit-event schema is defined under
the `contracts-artifacts` profile (`contracts/audit/` once that
namespace is introduced).

## Change Control

This file is a living document; it must be updated in the same change
set as any change to `jansu-sans-io/src/acl.rs` or
`jansu-sans-io/src/resource.rs`. The proof lane is `security`, and the
owner is Platform Engineering.
