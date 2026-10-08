# Runbook

All commands assume the Compose stack: `QC="docker compose run --rm tools"`. On bare metal, use `qc ...` with the `QC_*` variables exported.

## Health at a glance

```bash
$QC ops status --writer-url http://writer:9405   # exit 1 if anything is wrong
curl -s localhost:9405/metrics | grep qc_writer   # Prometheus metrics of the writer
$QC cdc status                                    # connector + replication slot
```

| Signal | Healthy | Where |
|---|---|---|
| Connector and task state | `RUNNING` | `cdc status`, Dagster `cdc_health` (every 5 min) |
| Slot `confirmed_lag_bytes` | under a few MB, and falling | `cdc status` |
| Slot `wal_status` | `reserved` | `lost` means the slot fell behind `max_slot_wal_keep_size`; see re-snapshot below |
| `qc_writer_offsets_behind_latest{stat="max"}` | 0, or a small backlog | writer `/metrics` |
| Silver freshness of hot tables | under 15 min | Dagster asset checks on `silver/*` |
| `bronze._dead_letters` | 0 rows | `ops status` |

The Dagster run-failure sensor posts failed runs to `QC_ALERT_WEBHOOK_URL` when it is set.

## Writer is down or lagging

1. `docker compose logs writer --tail 200`. Each batch logs `batch committed` with `table_seconds`.
2. A restart is always safe: the writer resumes from its checkpoint and re-applies any partial batch idempotently. Run `docker compose restart writer`.
3. Persistent lag: raise `QC_WRITER_MAX_OFFSETS_PER_TRIGGER` (fewer, larger MERGEs), or give the writer more cores (`QC_WRITER_SPARK_MASTER=local[4]`). Most of a batch's cost is fixed per-table MERGE overhead.
4. Commit conflicts with maintenance are retried automatically (`qc_writer_merge_retries_total`).

## Replaying topics / rebuilding silver

Reprocessing is idempotent, so a full replay is a supported operation:

```bash
docker compose stop writer
docker compose run --rm --no-deps --entrypoint rm writer -rf /data/checkpoints/cdc-writer
docker compose up -d writer          # re-reads every topic from the earliest retained offset
$QC reconcile --mode exact
```

To rebuild a corrupted silver table, drop it in Trino (`DROP TABLE lakehouse.silver.orders`), then replay. The writer recreates the table. This requires the topic to still hold the full history (retention: 7 days). For anything older, re-snapshot.

## Re-snapshot a table (no downtime)

Use this after retention loss, a lost slot, or a suspected silver drift. Debezium's incremental snapshot re-reads tables in chunks while streaming continues:

```bash
$QC cdc snapshot --tables commerce.orders commerce.order_items
```

Snapshot reads carry a current LSN, so they overwrite older silver state. They do **not** emit deletes for rows that vanished while CDC was broken. Run `reconcile --mode exact`; if it reports extra silver rows, tombstone them with a Trino `UPDATE ... SET _is_deleted = true WHERE <pk> NOT IN (...)` using the Postgres key list, or rebuild the table from an empty silver table.

## Replication slot problems

* **Slot inactive, WAL growing**: the connector is down. `docker compose restart connect`, then check `cdc status`. `max_slot_wal_keep_size=2GB` caps disk growth; past that the slot is invalidated (`wal_status = lost`).
* **Slot lost**: delete the connector and slot, then start again with a fresh snapshot:
  ```bash
  curl -X DELETE localhost:8083/connectors/qc-oltp-postgres
  docker compose exec postgres psql -U postgres -d qcommerce -c "select pg_drop_replication_slot('qc_cdc_slot')"
  $QC cdc register        # snapshot.mode=initial re-reads every table
  ```
  Then replay or re-snapshot as above, and reconcile.
* **Decommissioning**: always drop the slot. An abandoned slot retains WAL indefinitely.

## Schema changes

* **Additive (new nullable column)**: needs no action. Ship the migration and the writer evolves bronze and silver automatically (see `V002__orders_tip_amount.sql`). Use the `optional_column` macro in staging if dbt must run before the change reaches every environment.
* **Widening** (int→bigint, numeric(8,2)→numeric(10,2)): handled automatically.
* **New table**: add it to the publication in the same migration (`ALTER PUBLICATION qc_cdc_pub ADD TABLE ...`). The writer discovers the new topic within about 15 seconds and creates its tables.
* **Rename or type change**: these are breaking. Add a new column, backfill it, switch readers, then drop the old column. Silver keeps the old column; new rows have null in it.
* **Incompatible change**: the writer fails the batch with `UnsupportedSchema` and stops before writing anything wrong. Fix forward (new column), or recreate the affected bronze/silver table and replay.

## Failed MERGE / writer crash mid-batch

There is nothing to clean up. Bronze and silver commits are atomic per table, and the batch's offsets are not committed. On restart the whole batch runs again: bronze skips events it already has, and silver ignores LSNs it already has. The e2e test SIGKILLs the writer mid-stream to prove this.

## Dead letters

```sql
select reason, topic, count(*) from lakehouse.bronze._dead_letters group by 1, 2;
```

Causes: an unparseable envelope, an unknown op, or a missing LSN or key. After a fix, the affected records can be re-produced from the source with an incremental snapshot. Dead letters are deduplicated by (topic, partition, offset).

## Table maintenance

Dagster `iceberg_maintenance` runs daily at 03:30 IST: `optimize`, `expire_snapshots(7d)`, `remove_orphan_files(7d)` on every bronze and silver table. Run it ad hoc with `$QC maintenance run`. Silver tombstone purge is opt-in (`--purge-tombstones-days N`). N must exceed the Kafka retention, so a replay can never resurrect a purged row.

## Pausing the workload

`$QC generator pause` / `resume`. Exact reconciliation does this automatically and always resumes, even when it fails.
