"""Spark Structured Streaming job: Debezium topics in Kafka -> Iceberg bronze + silver.

Delivery semantics: Spark checkpoints Kafka offsets and re-runs a micro-batch after any failure, and
Debezium itself is at-least-once. Every write below is idempotent (see :mod:`qcommerce.lakehouse.iceberg`),
so the end-to-end result is effectively exactly-once.
"""

from __future__ import annotations

import json
import signal
import time
from collections import Counter
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from qcommerce import log
from qcommerce.lakehouse import connect_schema as cs
from qcommerce.lakehouse import iceberg, tables
from qcommerce.lakehouse.changes import TableChanges, parse_topic, survey
from qcommerce.lakehouse.metrics import WriterMetrics, serve
from qcommerce.lakehouse.spark import build_session
from qcommerce.settings import Settings

logger = log.get(__name__)

DEAD_LETTER_TABLE = "_dead_letters"
DEAD_LETTER_STRUCT = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("reason", StringType()),
        StructField("_ingested_at", TimestampType()),
    ]
)


class CdcBatchWriter:
    """The ``foreachBatch`` function.

    SQL runs on the micro-batch's own session (``batch.sparkSession``): temp views registered from a
    streaming micro-batch live there, not in the session that started the query.
    """

    def __init__(self, settings: Settings, metrics: WriterMetrics) -> None:
        self.catalog = settings.lake.catalog
        self.bronze_ns = settings.lake.bronze_namespace
        self.silver_ns = settings.lake.silver_namespace
        self.retries = settings.writer.merge_retries
        self.metrics = metrics
        self._known_schemas: dict[str, StructType] = {}

    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_ns}.{iceberg.q(table)}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_ns}.{iceberg.q(table)}"

    def _ensure(self, spark: SparkSession, fqn: str, struct: StructType, **kwargs) -> int:
        """Create/evolve ``fqn`` unless this process already knows it can hold ``struct``.
        Returns the number of schema changes (creation excluded)."""
        known = self._known_schemas.get(fqn)
        if known is not None and all(f in known.fields for f in struct.fields):
            return 0
        made = iceberg.ensure_table(spark, fqn, struct, **kwargs)
        self._known_schemas[fqn] = spark.table(fqn).schema
        return len([m for m in made if not m.startswith("create")])

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        spark = batch.sparkSession
        ingested_at = datetime.now(timezone.utc)
        raw = batch.select(
            "topic",
            "partition",
            "offset",
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            surveys = survey(raw)  # one pass: schemas, op counts, LSN range per topic
            if not surveys:
                return
            by_table = {tables.table_from_topic(t): t for t in surveys}
            events: Counter[tuple[str, str]] = Counter()
            timings: dict[str, float] = {}
            dead_letters = schema_changes = 0
            for table in tables.ordered(list(by_table)):
                info = surveys[by_table[table]]
                if table in tables.IGNORED or info.records == info.tombstones:
                    continue
                table_started = time.monotonic()
                records = raw.where(F.col("topic") == by_table[table])
                changes = parse_topic(records, table, batch_id, ingested_at, info)
                if changes is None:  # no record carried a readable schema: anything non-tombstone is garbage
                    dead_letters += self._write_dead_letters(
                        spark,
                        records.where(F.col("value").isNotNull()).select(
                            "topic",
                            "partition",
                            "offset",
                            "key",
                            "value",
                            F.lit("unparseable_envelope").alias("reason"),
                            F.lit(ingested_at).alias("_ingested_at"),
                        ),
                    )
                    continue
                changes.frame = changes.frame.persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    if info.suspicious:
                        dead_letters += self._write_dead_letters(spark, changes.dead_letters)
                    if info.events:
                        # Bronze first: the change log is always a superset of what silver reflects.
                        schema_changes += self._write_bronze(spark, changes, info.min_lsn or 0)
                        schema_changes += self._write_silver(spark, changes, truncated="t" in info.ops)
                    elif "t" in info.ops:
                        schema_changes += self._write_silver(spark, changes, truncated=True)
                finally:
                    changes.frame.unpersist()
                for op, n in info.ops.items():
                    events[(table, op)] += n
                timings[table] = round(time.monotonic() - table_started, 2)
            elapsed = time.monotonic() - started
            self.metrics.record_batch(batch_id, elapsed, events, dead_letters, schema_changes)
            logger.info(
                "batch committed",
                extra={
                    "batch_id": batch_id,
                    "seconds": round(elapsed, 2),
                    "records": sum(i.records for i in surveys.values()),
                    "events": sum(events.values()),
                    "dead_letters": dead_letters,
                    "schema_changes": schema_changes,
                    "table_seconds": timings,
                },
            )
        finally:
            raw.unpersist()

    # ------------------------------------------------------------------ dead letters

    def _write_dead_letters(self, spark: SparkSession, frame: DataFrame) -> int:
        frame = frame.select(
            *[F.col(f.name).cast(f.dataType).alias(f.name) for f in DEAD_LETTER_STRUCT.fields]
        )
        rows = frame.count()
        if rows == 0:
            return 0
        target = self.bronze(DEAD_LETTER_TABLE)
        self._ensure(
            spark,
            target,
            DEAD_LETTER_STRUCT,
            partitioned_by="days(_ingested_at)",
            properties=tables.BRONZE_PROPERTIES,
            comment="CDC records the writer could not apply",
        )
        frame.createOrReplaceTempView("qc_dead_letters_batch")
        iceberg.run_with_retry(
            spark,
            (
                f"MERGE INTO {target} t USING qc_dead_letters_batch s "
                "ON t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset "
                "WHEN NOT MATCHED THEN INSERT *"
            ),
            retries=self.retries,
            on_retry=self.metrics.record_retry,
        )
        logger.warning("dead-lettered records", extra={"count": rows})
        return rows

    # ------------------------------------------------------------------ bronze

    def _write_bronze(self, spark: SparkSession, changes: TableChanges, min_lsn: int) -> int:
        target = self.bronze(changes.table)
        struct = iceberg.bronze_struct(changes.schema)
        evolved = self._ensure(
            spark,
            target,
            struct,
            partitioned_by=tables.BRONZE_PARTITIONING,
            properties=tables.BRONZE_PROPERTIES,
            comment=f"Change log of commerce.{changes.table} (one row per CDC event)",
        )
        view = f"qc_bronze_{changes.table}"
        dedup_key = [*changes.schema.primary_key, "_lsn", "_op"]
        changes.rows.dropDuplicates(dedup_key).createOrReplaceTempView(view)
        sql = iceberg.bronze_merge_sql(
            target, view, changes.schema.primary_key, [f.name for f in struct.fields], min_lsn
        )
        iceberg.run_with_retry(spark, sql, retries=self.retries, on_retry=self.metrics.record_retry)
        return evolved

    # ------------------------------------------------------------------ silver

    def _write_silver(self, spark: SparkSession, changes: TableChanges, *, truncated: bool) -> int:
        target = self.silver(changes.table)
        struct = iceberg.silver_struct(changes.schema)
        partition = tables.SILVER_PARTITIONING.get(changes.table)
        evolved = self._ensure(
            spark,
            target,
            struct,
            partitioned_by=partition,
            properties=tables.SILVER_PROPERTIES,
            comment=f"Current state of commerce.{changes.table} (tombstoned deletes)",
        )
        rows = changes.rows
        if truncated:
            last_truncate = max(lsn for lsn, _ in changes.truncates())
            iceberg.run_with_retry(
                spark,
                iceberg.truncate_sql(target, last_truncate),
                retries=self.retries,
                on_retry=self.metrics.record_retry,
            )
            rows = rows.where(F.col("_lsn") > last_truncate)  # anything older is wiped by the truncate
            logger.warning("applied source TRUNCATE", extra={"table": changes.table, "lsn": last_truncate})
        pk = list(changes.schema.primary_key)
        newest_first = Window.partitionBy(*pk).orderBy(F.col("_lsn").desc(), F.col("_kafka_offset").desc())
        latest = (
            rows.withColumn("_rank", F.row_number().over(newest_first))
            .where(F.col("_rank") == 1)
            .select(*changes.schema.names, "_op", "_lsn", "_tx_id", "_source_ts", "_ingested_at")
        )
        view = f"qc_silver_{changes.table}"
        latest.createOrReplaceTempView(view)
        string_columns = [
            c.name for c in changes.schema.columns if isinstance(cs.decoded_type(c), StringType)
        ]
        sql = iceberg.silver_merge_sql(target, view, pk, changes.schema.names, string_columns)
        iceberg.run_with_retry(spark, sql, retries=self.retries, on_retry=self.metrics.record_retry)
        return evolved


class ProgressListener(StreamingQueryListener):
    def __init__(self, metrics: WriterMetrics) -> None:
        self.metrics = metrics

    def onQueryStarted(self, event) -> None:
        logger.info("streaming query started", extra={"query_id": str(event.id), "query_name": event.name})

    def onQueryProgress(self, event) -> None:
        progress = json.loads(event.progress.json)
        self.metrics.record_progress(progress)
        behind_max, _ = self.metrics.offsets_behind()
        logger.info(
            "streaming progress",
            extra={
                "batch_id": progress.get("batchId"),
                "input_rows": progress.get("numInputRows"),
                "input_rows_per_second": progress.get("inputRowsPerSecond"),
                "processed_rows_per_second": progress.get("processedRowsPerSecond"),
                "max_offsets_behind_latest": behind_max,
            },
        )

    def onQueryIdle(self, event) -> None:
        return

    def onQueryTerminated(self, event) -> None:
        logger.info(
            "streaming query terminated", extra={"query_id": str(event.id), "exception": event.exception}
        )


def run(settings: Settings, *, available_now: bool = False) -> None:
    """Start the writer. ``available_now`` processes everything currently in Kafka, then exits."""
    writer_cfg = settings.writer
    spark = build_session(settings, "qc-cdc-writer")
    spark.sparkContext.setLogLevel("WARN")
    for namespace in (settings.lake.bronze_namespace, settings.lake.silver_namespace):
        iceberg.ensure_namespace(spark, settings.lake.catalog, namespace)

    metrics = WriterMetrics()
    server = serve(metrics, writer_cfg.metrics_port)
    spark.streams.addListener(ProgressListener(metrics))

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribePattern", settings.kafka.cdc_topic_pattern)
        .option("startingOffsets", writer_cfg.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(writer_cfg.max_offsets_per_trigger))
        .option("kafka.metadata.max.age.ms", "15000")
        .load()
    )
    stream = (
        source.writeStream.queryName("qc_cdc_writer")
        .option("checkpointLocation", writer_cfg.checkpoint_location)
        .foreachBatch(CdcBatchWriter(settings, metrics))
    )
    stream = (
        stream.trigger(availableNow=True)
        if available_now
        else stream.trigger(processingTime=f"{writer_cfg.trigger_seconds} seconds")
    )
    query = stream.start()
    logger.info(
        "writer started",
        extra={
            "topics": settings.kafka.cdc_topic_pattern,
            "checkpoint": writer_cfg.checkpoint_location,
            "available_now": available_now,
            "metrics_port": writer_cfg.metrics_port,
        },
    )

    def _stop(signum, _frame) -> None:
        logger.info("stopping writer", extra={"signal": signum})
        query.stop()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        query.awaitTermination()
    finally:
        server.shutdown()
        spark.stop()
    if query.exception() is not None:
        raise SystemExit(f"writer failed: {query.exception()}")
