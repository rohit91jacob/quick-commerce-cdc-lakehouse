# CDC semantics: ordering, delivery and correctness

This page explains why silver equals Postgres even though every hop delivers *at least once*.

## The chain

| Hop | Guarantee | Why |
|---|---|---|
| Postgres → Debezium | Every committed change, in commit order | Logical decoding (`pgoutput`) of the WAL through the replication slot `qc_cdc_slot`, publication `qc_cdc_pub` |
| Debezium → Kafka | At least once, ordered per key | One topic per table. Messages are keyed by primary key, so all changes to a row land on the same partition in order. After a crash Debezium resumes from the last *committed* offset and can re-send events |
| Kafka → Spark | At least once per micro-batch | Offsets are checkpointed only after `foreachBatch` returns. A failed batch is re-run with the same offsets |
| Spark → Iceberg | Idempotent writes | See below |

Every hop can repeat work, and none can lose it (slot + Kafka retention + checkpoint). The writes are idempotent, so the end-to-end result is **effectively exactly-once**.

## Event identity and order

Each change event carries `source.lsn`: the WAL position of the change. LSNs grow with commit order and are unique per change. That gives us:

* **Identity**: `(primary key, LSN, op)`. Snapshot reads (`op = r`) share the snapshot LSN but differ by key.
* **Order**: for one row, a higher LSN is always newer. This holds across the initial snapshot and streaming, because streaming starts at the slot's consistent point.

## Bronze: the change log

`lakehouse.bronze.<table>` has one row per change event: business columns, then `_op`, `_lsn`, `_tx_id`, `_source_ts` (commit time), `_event_ts`, `_snapshot`, Kafka coordinates, `_batch_id` and `_ingested_at`.

```sql
MERGE INTO bronze.t USING batch s
ON t.pk = s.pk AND t._lsn = s._lsn AND t._op = s._op AND t._lsn >= <min LSN in batch>
WHEN NOT MATCHED THEN INSERT ...
```

* A replayed event already exists, so it is not inserted again.
* An insert-only MERGE runs as an anti-join plus append, with no rewrites.
* The `t._lsn >= min` term lets Iceberg prune data files using column statistics.

## Silver: current state with tombstones

The batch is first reduced to the newest event per primary key (highest LSN). Then:

```sql
MERGE INTO silver.t USING latest s ON t.pk = s.pk
WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN UPDATE SET _is_deleted = true, _lsn = s._lsn, ...
WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET <all columns>, _is_deleted = false, _lsn = s._lsn, ...
WHEN NOT MATCHED THEN INSERT (..., _is_deleted = (s._op = 'd'))
```

| Situation | Outcome |
|---|---|
| Duplicate event (same LSN) | `s._lsn > t._lsn` is false, so nothing changes |
| Stale update arriving after a newer one | Lower LSN, so it is ignored |
| Delete | The row becomes a tombstone that keeps its last values and the delete's LSN |
| Older insert arriving after the delete | Lower LSN than the tombstone, so the row is **not** resurrected |
| Delete of a row never seen | A tombstone is inserted, which blocks any older insert that arrives later |
| Re-insert after delete (e.g. a relisted SKU) | Higher LSN, so the row comes back to life |
| Primary key update | Debezium emits a delete for the old key and a create for the new key |
| `TRUNCATE` | Every row with an older LSN is tombstoned; older events in the same batch are dropped |
| Unchanged TOASTed value (`__debezium_unavailable_value`) | Silver keeps its existing value |

Consumers read `WHERE NOT _is_deleted`. The dbt staging models do this. A maintenance option purges tombstones older than the replay horizon.

These cases are covered by `tests/spark/test_cdc_writer.py`, using real Iceberg tables.

## Schema changes

Every message embeds its Connect schema. In each batch the writer:

1. collects the distinct schemas for each topic in one survey pass
2. merges them: columns keep their order, and new columns go last
3. compares the result with the Iceberg table

New columns get `ALTER TABLE ... ADD COLUMN` in bronze and silver. Iceberg-legal widenings (int→long, float→double, larger decimal precision) get `ALTER COLUMN ... TYPE`. Incompatible changes stop the batch with an explicit error; the runbook covers what to do. Dropped source columns stay in Iceberg and are null from then on.

Older rows read as null in the new column. The dbt `optional_column` macro lets `stg_orders` reference `tip_amount` before the column exists anywhere.

## Cross-table consistency

A micro-batch commits tables one by one, in dependency order: parents before children (`tables.LOAD_ORDER`). A reader can therefore see a new order before its payment, but never an order item without its order. That is fine for the 15-minute dbt cadence. When a strictly consistent cut is needed, reconciliation's fence (next section) provides one.

## Proving it: reconciliation fences

`qc reconcile --mode exact`:

1. pauses the generator through `ops.generator_control` and waits for it to acknowledge
2. **fence 1**: updates `commerce.cdc_heartbeat` (id 2) and waits until the slot's `confirmed_flush_lsn` passes it. Debezium confirms an LSN only after Kafka has acknowledged every earlier record, so everything before the fence is now durably in Kafka
3. **fence 2**: writes a new token and waits until it is visible in `silver.cdc_heartbeat`. The micro-batch that carried it was planned after fence 1, so it included everything before fence 1
4. profiles every table on both sides in one `REPEATABLE READ` snapshot: row count; then per column the non-null count, sums (integers, decimals), total text length, true-count and min/max timestamps. Floating-point sums are compared with a 1e-6 relative tolerance

`--mode settled` skips the pause. It compares only rows with `updated_at` older than `now - settle`, after a single fence.
