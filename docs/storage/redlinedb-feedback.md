# RedlineDB Backend Feedback

Observations collected on 2026-05-17 while making `redlinedb` the primary `jansu-storage` backend and deprecating `postgres` + `sqlite/lite/libsql/turso/limbo` options.

## Summary

`redlinedb` is the chosen single-backend path for jansu storage going forward, alongside the object-store backends (`dynostore`, `slatedb`). This document captures shape, signal, and ergonomic observations from working through the codebase. It is not a defect list — it is a punch list of opportunities the next refactor or upstream `redlinedb` release should consider.

## Code shape (jansu-storage/src/redlinedb)

- 20 Rust files, ~6,863 LOC total.
- 41 `.sql` files embedded via `include_str!` in the same directory as the Rust source.
- Files >400 LOC (auditor-relevant for jankurai shape dimension):

  | File | LOC |
  |---|---:|
  | `storage_admin.rs` | 574 |
  | `redline.rs` | 528 |
  | `mod.rs` | 479 |
  | `engine.rs` | 451 |
  | `builder.rs` | 437 |
  | `storage_transactions.rs` | 414 |
  | `storage_offsets.rs` | 403 |

- `mod.rs` at 479 LOC contains the `Txn` struct + `TryFrom<Row>` impl + a long `use` block + module re-exports. Worth splitting `Txn` into its own module so `mod.rs` stays a thin facade.
- `storage_admin.rs` (574) is the single largest file in the backend; likely splittable along `topic` / `producer` / `consumer-group` admin axes.

## Signal counts (redlinedb only, non-test)

| Pattern | Count | Rule |
|---|---:|---|
| `.clone()` | 33 | `rust.review.clone-overuse` |
| ` as ` | 29 | `rust.review.as-cast` |
| `expect(` | 6 | `rust.review.generic-unwrap` |
| `Arc<Mutex<…>>` | 6 | `rust.review.arc-mutex-default` |
| `unwrap(` | 0 | clean |

Low compared to the wider workspace (which had 712 `.clone()`, 485 ` as `, 133 unwrap/expect across non-test Rust). RedlineDB is already in good shape on these axes. The 33 `.clone()` and 29 `as` casts are tractable.

## Ergonomic friction observed

### 1. SQL string templates
- All queries are `include_str!("foo.sql")` against the `redlinedb` engine.
- No compile-time schema verification — bad SQL only fails at runtime, often deep inside an async path.
- Filenames are clear (`policy_compact_distinct_k.sql`, `record_fetch_keyed.sql`) but column changes still require manual coordination across SQL + Rust types.
- Consideration: a `sqlx::query!`-style macro or a thin schema-binding type for each query would catch errors at `cargo build`.

### 2. Many narrow storage_* modules
- `storage_admin.rs`, `storage_describe.rs`, `storage_groups.rs`, `storage_list_offsets.rs`, `storage_metadata.rs`, `storage_misc.rs`, `storage_offsets.rs`, `storage_producer.rs`, `storage_produce.rs`, `storage_transactions.rs` all delegate to `Engine` via similar shapes.
- The `delegate_helpers.rs` + `delegate_compaction.rs` + `delegate_produce_core.rs` files suggest macro consolidation was attempted partially.
- Opportunity: a single `impl Storage for Engine` block with one shared dispatch macro, mirroring the pattern in `jansu-storage/src/service/request_channel/storage.rs` from the parallel-refactor work.

### 3. `Arc<Mutex<…>>` in pool/engine
- 6 instances suggest shared mutable state across async tasks.
- Most are legitimate concurrency primitives but each is a candidate for either `RwLock` (read-heavy) or `arc-swap` (snapshot semantics).
- Worth a focused pass to document the contention model per `Mutex`.

### 4. `redline_timestamp.rs`
- 104-line wrapper around `chrono::NaiveDateTime` — small, but its existence implies upstream `redlinedb` does not yet expose first-class timestamp types.
- Upstream feedback: consider a typed `RedlineTimestamp` newtype in the `redlinedb` crate itself so downstream users do not need this adapter.

## Operational observations

- The 41 SQL files are tightly coupled to redlinedb's table schema. Any schema migration in the redlinedb crate will silently invalidate them — needs an integration test that runs `include_str!` queries against a known-good DB at CI time.
- `pool.rs` (243 LOC) appears to wrap a connection pool with hand-rolled cancellation. Worth verifying it gracefully handles shutdown under load (postmortem-worthy: was there an incident where in-flight queries stranded a worker on shutdown?).
- The `Engine::record(&self, start: SystemTime, op: &'static str)` pattern in `engine.rs` is a simple per-op metric recorder. Consider tracing spans (`#[instrument]`) for consistency with the rest of the workspace.

## Upstream `redlinedb` crate asks

If feedback is being collected for the `redlinedb` crate itself (rather than the integration here):

1. **Typed timestamps**: expose a `RedlineTimestamp` newtype so consumers do not need to wrap `chrono::NaiveDateTime`.
2. **`sqlx`-style query macros**: provide a `redlinedb::query!` macro that verifies queries at compile time against a schema fixture.
3. **`Stream`-based fetch API**: today's fetch path materializes into `Vec`s. A `Stream` would let large fetches honor backpressure.
4. **First-class metrics hooks**: expose per-op latency/error counters so each consumer does not have to roll its own `record(start, op)` helper.
5. **Documented shutdown ordering**: a section in the crate README on the right teardown sequence for `Engine` + `Pool` would prevent reinventing the cancellation logic in each consumer.

## Action items for jansu after deprecating postgres + sqlite

1. Split `redlinedb/mod.rs` (479) and `storage_admin.rs` (574) to fall back under the 500 LOC line, closing the jankurai shape finding for redlinedb specifically.
2. Sweep `.clone()` (33) and ` as ` casts (29) following the same pattern as the parallel refactor on `jansu-sans-io` (use `Arc::clone(&x)` and `u32::from(x)` / `try_from`).
3. Drop the 6 `expect()` calls in favor of typed errors via `?` where the surrounding fn already returns `Result`.
4. Add a CI gate that fails if `postgres`, `tokio-postgres`, `deadpool`, `lite`, `libsql`, or `turso` reappears in workspace product code.

## Migration impact (recorded for the deprecation commit)

| Removed | Files | LOC |
|---|---:|---:|
| `pg/` directory (18 files + 3 SQL) + postgres feature + 2 pg-only broker tests + 13 in-file `mod pg { }` blocks across broker tests + pg variants in storage tests + pg dispatch arms + lib.rs/error.rs/capabilities.rs pg paths | 25 deleted, 32 modified | -7552 net (commit 1373402) |
| sqlite/lite/libsql/turso/limbo references | already removed in WIP checkpoint | already absorbed |

Keep: `redlinedb` (primary), `dynostore` (object store), `slatedb` (object KV).
