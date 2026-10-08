"""Turn one topic's slice of a Kafka micro-batch into typed change rows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from qcommerce.lakehouse import connect_schema as cs

OPS = ("c", "u", "d", "r")

# Only the parts of Debezium's `source` block the pipeline uses.
SOURCE_STRUCT = StructType(
    [
        StructField("lsn", LongType()),
        StructField("txId", LongType()),
        StructField("ts_ms", LongType()),
        StructField("snapshot", StringType()),
    ]
)

# Metadata columns carried by every bronze change row (business columns come first).
BRONZE_META = (
    "_op",
    "_lsn",
    "_tx_id",
    "_source_ts",
    "_event_ts",
    "_snapshot",
    "_kafka_partition",
    "_kafka_offset",
    "_batch_id",
    "_ingested_at",
)


@dataclass
class TableChanges:
    """Parsed records of one table. ``frame`` is parsed once (and persisted by the writer); the
    properties below are views over it."""

    table: str
    schema: cs.TableSchema
    frame: DataFrame

    @property
    def rows(self) -> DataFrame:
        """Valid c/u/d/r events: business columns + BRONZE_META."""
        return self.frame.where(F.col("_reason").isNull() & F.col("_op").isin(*OPS)).select(
            *self.schema.names, *BRONZE_META
        )

    @property
    def dead_letters(self) -> DataFrame:
        return self.frame.where(F.col("_reason").isNotNull()).select(
            F.col("_topic").alias("topic"),
            F.col("_kafka_partition").alias("partition"),
            F.col("_kafka_offset").alias("offset"),
            F.col("_dl_key").alias("key"),
            F.col("_dl_value").alias("value"),
            F.col("_reason").alias("reason"),
            "_ingested_at",
        )

    def truncates(self) -> list[tuple[int, datetime]]:
        """(lsn, source timestamp) of TRUNCATE events."""
        return [
            (r._lsn, r._source_ts)
            for r in self.frame.where(F.col("_op") == "t").select("_lsn", "_source_ts").collect()
        ]


@dataclass
class TopicSurvey:
    """What one cheap pass over a micro-batch learned about a topic."""

    value_schemas: list[dict] = field(default_factory=list)
    key_schemas: list[dict] = field(default_factory=list)
    ops: dict[str, int] = field(default_factory=dict)
    min_lsn: int | None = None
    records: int = 0
    tombstones: int = 0
    suspicious: int = 0  # records that will be dead-lettered (unparseable, unknown op, no LSN)

    @property
    def events(self) -> int:
        return sum(n for op, n in self.ops.items() if op in OPS)


def survey(raw: DataFrame) -> dict[str, TopicSurvey]:
    """One job over the whole batch: distinct schemas, op counts and min LSN per topic."""
    top = F.json_tuple(F.col("value"), "schema", "payload").alias("vs", "payload")
    rows = (
        raw.select("topic", "key", "value", top)
        .select(
            "topic",
            "vs",
            F.get_json_object("key", "$.schema").alias("ks"),
            F.get_json_object("payload", "$.op").alias("op"),
            F.get_json_object("payload", "$.source.lsn").cast(LongType()).alias("lsn"),
            F.col("value").isNull().alias("tombstone"),
            F.col("key").isNull().alias("keyless"),
        )
        .groupBy("topic", "vs", "ks", "op", "tombstone", "keyless")
        .agg(F.count("*").alias("n"), F.min("lsn").alias("min_lsn"), F.count("lsn").alias("with_lsn"))
        .collect()
    )
    out: dict[str, TopicSurvey] = {}
    for r in rows:
        info = out.setdefault(r.topic, TopicSurvey())
        info.records += r.n
        if r.tombstone:
            info.tombstones += r.n
            continue
        if r.vs:
            schema = json.loads(r.vs)
            if schema not in info.value_schemas:
                info.value_schemas.append(schema)
        if r.ks:
            key_schema = json.loads(r.ks)
            if key_schema not in info.key_schemas:
                info.key_schemas.append(key_schema)
        op = r.op or "?"
        info.ops[op] = info.ops.get(op, 0) + r.n
        if r.vs is None or op not in (*OPS, "t"):
            info.suspicious += r.n
        elif op in OPS:
            info.suspicious += (r.n - r.with_lsn) + (r.n if r.keyless else 0)
            if r.min_lsn is not None:
                info.min_lsn = r.min_lsn if info.min_lsn is None else min(info.min_lsn, r.min_lsn)
    return out


def parse_topic(
    raw: DataFrame, table: str, batch_id: int, ingested_at: datetime, info: TopicSurvey
) -> TableChanges | None:
    """``raw`` holds one topic's records: topic, partition, offset, key (string), value (string)."""
    events = raw.where(F.col("value").isNotNull())  # tombstones follow deletes; nothing to apply
    if not info.value_schemas:
        return None
    columns = cs.merge_columns(cs.envelope_columns(s) for s in info.value_schemas)
    key_fields = cs.merge_columns(cs.key_columns(s) for s in info.key_schemas)
    schema = cs.TableSchema(columns, tuple(f.name for f in key_fields))

    row_struct = cs.raw_type(cs.ConnectField("row", "struct", fields=columns))
    payload = StructType(
        [
            StructField("before", row_struct),
            StructField("after", row_struct),
            StructField("source", SOURCE_STRUCT),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
        ]
    )
    key_struct = cs.raw_type(cs.ConnectField("key", "struct", fields=key_fields))
    parsed = events.select(
        "topic",
        "partition",
        "offset",
        "key",
        "value",
        F.from_json("value", StructType([StructField("payload", payload)])).getField("payload").alias("p"),
        F.from_json("key", StructType([StructField("payload", key_struct)])).getField("payload").alias("k"),
    )
    op = F.col("p.op")
    pk_present: Column = F.lit(True)
    for k in schema.primary_key:
        pk_present = pk_present & F.col("k").getField(k).isNotNull()
    reason = (
        F.when(F.col("p").isNull(), F.lit("unparseable_envelope"))
        .when(op == "t", F.lit(None).cast(StringType()))
        .when(~op.isin(*OPS), F.concat(F.lit("unknown_op:"), F.coalesce(op, F.lit("null"))))
        .when(F.col("p.source.lsn").isNull(), F.lit("missing_lsn"))
        .when(~pk_present, F.lit("missing_primary_key"))
    )

    business = []
    for col in columns:
        if col.name in schema.primary_key:
            raw_value = F.col("k").getField(col.name)
        else:  # deletes carry only the key (default REPLICA IDENTITY), so other columns are null
            raw_value = F.when(op == "d", F.lit(None)).otherwise(F.col("p.after").getField(col.name))
        business.append(cs.decode(raw_value, col).alias(col.name))
    frame = parsed.select(
        *business,
        op.alias("_op"),
        F.col("p.source.lsn").alias("_lsn"),
        F.col("p.source.txId").alias("_tx_id"),
        F.timestamp_millis("p.source.ts_ms").alias("_source_ts"),
        F.timestamp_millis("p.ts_ms").alias("_event_ts"),
        F.col("p.source.snapshot").alias("_snapshot"),
        F.col("partition").cast(LongType()).alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.lit(batch_id).cast(LongType()).alias("_batch_id"),
        F.lit(ingested_at).alias("_ingested_at"),
        reason.alias("_reason"),
        F.col("topic").alias("_topic"),
        # Raw payloads are kept only for records that will be dead-lettered.
        F.when(reason.isNotNull(), F.col("key")).alias("_dl_key"),
        F.when(reason.isNotNull(), F.col("value")).alias("_dl_value"),
    )
    return TableChanges(table, schema, frame)
