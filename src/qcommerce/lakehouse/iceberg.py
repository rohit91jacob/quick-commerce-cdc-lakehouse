"""Iceberg DDL (create / evolve) and the idempotent MERGE statements behind bronze and silver.

Bronze (``<catalog>.bronze.<table>``) is the append-only change log: one row per change event,
deduplicated on (primary key, LSN, op) so at-least-once delivery, writer restarts and full replays never
duplicate history.

Silver (``<catalog>.silver.<table>``) is the current state, one row per primary key. A change is applied
only if its LSN is newer than the row's ``_lsn``, which makes the MERGE commutative and idempotent:
duplicates and stale (out-of-order) events are no-ops. Deletes become tombstones (``_is_deleted = true``,
keeping the last LSN) instead of physical deletes, so a stale insert/update arriving after a delete
cannot resurrect the row. Consumers read ``WHERE NOT _is_deleted``.
"""

from __future__ import annotations

import time
from collections.abc import Sequence

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from qcommerce import log
from qcommerce.lakehouse import connect_schema as cs

logger = log.get(__name__)

TOAST = cs.TOAST_PLACEHOLDER
RETRYABLE_MARKERS = (
    "ValidationException",
    "CommitFailedException",
    "CommitStateUnknownException",
    "Found conflicting files",
    "Cannot commit",
)


def q(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def bronze_struct(schema: cs.TableSchema) -> StructType:
    fields = [StructField(c.name, cs.decoded_type(c), True) for c in schema.columns]
    fields += [
        StructField("_op", StringType()),
        StructField("_lsn", LongType()),
        StructField("_tx_id", LongType()),
        StructField("_source_ts", TimestampType()),
        StructField("_event_ts", TimestampType()),
        StructField("_snapshot", StringType()),
        StructField("_kafka_partition", LongType()),
        StructField("_kafka_offset", LongType()),
        StructField("_batch_id", LongType()),
        StructField("_ingested_at", TimestampType()),
    ]
    return StructType(fields)


def silver_struct(schema: cs.TableSchema) -> StructType:
    fields = [StructField(c.name, cs.decoded_type(c), True) for c in schema.columns]
    fields += [
        StructField("_op", StringType()),
        StructField("_lsn", LongType()),
        StructField("_tx_id", LongType()),
        StructField("_source_ts", TimestampType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_is_deleted", BooleanType()),
    ]
    return StructType(fields)


def ensure_namespace(spark: SparkSession, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def ensure_table(
    spark: SparkSession,
    fqn: str,
    struct: StructType,
    *,
    partitioned_by: str | None,
    properties: dict[str, str],
    comment: str,
) -> list[str]:
    """Create the table, or add/widen columns so it can hold ``struct``. Returns schema changes made."""
    if not spark.catalog.tableExists(fqn):
        cols = ",\n  ".join(f"{q(f.name)} {f.dataType.simpleString()}" for f in struct.fields)
        props = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
        partition = f"\nPARTITIONED BY ({partitioned_by})" if partitioned_by else ""
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fqn} (\n  {cols}\n) USING iceberg{partition}\n"
            f"COMMENT '{comment}'\nTBLPROPERTIES ({props})"
        )
        logger.info("created table", extra={"table": fqn})
        return [f"create {fqn}"]
    current = {f.name: f.dataType for f in spark.table(fqn).schema.fields}
    changes: list[str] = []
    for f in struct.fields:
        existing: DataType | None = current.get(f.name)
        if existing is None:
            spark.sql(f"ALTER TABLE {fqn} ADD COLUMN {q(f.name)} {f.dataType.simpleString()}")
            changes.append(f"add {f.name} {f.dataType.simpleString()}")
        elif existing != f.dataType:
            widened = cs.widen(existing, f.dataType)
            if widened is None:
                raise cs.UnsupportedSchema(f"{fqn}.{f.name}: cannot change {existing} to {f.dataType}")
            if widened != existing:
                spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN {q(f.name)} TYPE {widened.simpleString()}")
                changes.append(f"widen {f.name} to {widened.simpleString()}")
    if changes:
        logger.info("evolved table schema", extra={"table": fqn, "changes": changes})
    return changes


def bronze_merge_sql(
    target: str, source: str, primary_key: Sequence[str], columns: Sequence[str], min_lsn: int
) -> str:
    on = " AND ".join(
        [f"t.{q(k)} = s.{q(k)}" for k in primary_key]
        + ["t._lsn = s._lsn", "t._op = s._op", f"t._lsn >= {int(min_lsn)}"]  # last term: file pruning
    )
    cols = ", ".join(q(c) for c in columns)
    vals = ", ".join(f"s.{q(c)}" for c in columns)
    return f"MERGE INTO {target} t USING {source} s ON {on} WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"


def silver_merge_sql(
    target: str,
    source: str,
    primary_key: Sequence[str],
    columns: Sequence[str],
    string_columns: Sequence[str],
) -> str:
    """LSN-guarded upsert with tombstones. ``columns`` are the business columns present in ``source``."""
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in primary_key)
    meta = (
        "t._op = s._op, t._lsn = s._lsn, t._tx_id = s._tx_id, t._source_ts = s._source_ts, "
        "t._ingested_at = s._ingested_at"
    )
    strings = set(string_columns)

    def updated(c: str) -> str:
        # Unchanged TOASTed values arrive as a placeholder: keep what silver already has.
        if c in strings:
            return f"CASE WHEN s.{q(c)} = '{TOAST}' THEN t.{q(c)} ELSE s.{q(c)} END"
        return f"s.{q(c)}"

    def inserted(c: str) -> str:
        return f"CASE WHEN s.{q(c)} = '{TOAST}' THEN NULL ELSE s.{q(c)} END" if c in strings else f"s.{q(c)}"

    non_key = [c for c in columns if c not in primary_key]
    upsert = ", ".join([f"t.{q(c)} = {updated(c)}" for c in non_key] + [meta, "t._is_deleted = false"])
    insert_cols = ", ".join(
        [q(c) for c in columns] + ["_op", "_lsn", "_tx_id", "_source_ts", "_ingested_at", "_is_deleted"]
    )
    insert_vals = ", ".join(
        [inserted(c) for c in columns]
        + ["s._op", "s._lsn", "s._tx_id", "s._source_ts", "s._ingested_at", "s._op = 'd'"]
    )
    return (
        f"MERGE INTO {target} t USING {source} s ON {on}\n"
        f"WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN UPDATE SET {meta}, t._is_deleted = true\n"
        f"WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {upsert}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def truncate_sql(target: str, lsn: int) -> str:
    """A source TRUNCATE tombstones every row older than it."""
    return (
        f"UPDATE {target} SET _is_deleted = true, _op = 't', _lsn = {int(lsn)} "
        f"WHERE _lsn < {int(lsn)} AND NOT _is_deleted"
    )


def run_with_retry(spark: SparkSession, statement: str, *, retries: int, on_retry=None) -> None:
    """Execute ``statement``; retry optimistic-concurrency conflicts (e.g. with table maintenance).

    Safe because every statement this module produces is idempotent.
    """
    for attempt in range(retries + 1):
        try:
            spark.sql(statement)
            return
        except Exception as exc:  # py4j surfaces Java exceptions as generic errors
            message = str(exc)
            if attempt >= retries or not any(m in message for m in RETRYABLE_MARKERS):
                raise
            delay = min(30.0, 0.5 * 2**attempt)
            logger.warning("commit conflict; retrying", extra={"attempt": attempt + 1, "delay_s": delay})
            if on_retry is not None:
                on_retry()
            time.sleep(delay)
