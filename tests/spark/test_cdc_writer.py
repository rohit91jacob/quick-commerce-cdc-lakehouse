"""The writer's foreachBatch logic against a real (local) Iceberg catalog.

Each test feeds hand-built Debezium records through ``CdcBatchWriter`` exactly as Spark's Kafka source
would, then inspects bronze and silver. Covered: snapshot + streaming, updates, deletes and tombstones,
duplicates and full replays, out-of-order events, delete-before-insert, re-insert after delete,
composite keys, online schema change, TOASTed values, TRUNCATE and malformed records.
"""

from __future__ import annotations

import itertools
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from qcommerce.lakehouse.connect_schema import TOAST_PLACEHOLDER
from qcommerce.lakehouse.metrics import WriterMetrics
from qcommerce.lakehouse.writer import CdcBatchWriter
from qcommerce.settings import LakehouseSettings, WriterSettings
from tests import debezium as dbz

pytestmark = pytest.mark.spark

KAFKA_SCHEMA = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("key", StringType()),
        StructField("value", StringType()),
    ]
)

ORDERS = dbz.Table(
    "orders",
    ["order_id"],
    [
        dbz.int64("order_id"),
        dbz.text("status", optional=False),
        dbz.numeric("total", 10, 2),
        dbz.timestamptz("placed_at"),
        dbz.text("note"),
    ],
)
INVENTORY = dbz.Table(
    "inventory",
    ["store_id", "product_id"],
    [
        dbz.int32("store_id"),
        dbz.int32("product_id"),
        dbz.int32("on_hand", optional=True),
    ],
)


class _Settings:
    """Just the settings the writer reads, with a unique namespace per test."""

    def __init__(self, suffix: str) -> None:
        self.lake = LakehouseSettings(
            catalog="lakehouse", bronze_namespace=f"bronze_{suffix}", silver_namespace=f"silver_{suffix}"
        )
        self.writer = WriterSettings(merge_retries=2)


@pytest.fixture
def pipeline(spark):
    suffix = uuid.uuid4().hex[:8]
    settings = _Settings(suffix)
    for ns in (settings.lake.bronze_namespace, settings.lake.silver_namespace):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    writer = CdcBatchWriter(settings, WriterMetrics())
    offsets = itertools.count()
    batches = itertools.count()

    class Pipeline:
        metrics = writer.metrics

        def send(self, *records: tuple[dbz.Table, tuple[str, str | None]]) -> None:
            rows = [(table.topic, 0, next(offsets), key, value) for table, (key, value) in records]
            writer(spark.createDataFrame(rows, KAFKA_SCHEMA), next(batches))

        def silver(self, table: str) -> dict:
            frame = spark.table(f"lakehouse.{settings.lake.silver_namespace}.{table}")
            key = [c for c in ("order_id", "store_id", "product_id") if c in frame.columns]
            return {tuple(r[k] for k in key): r.asDict() for r in frame.collect()}

        def bronze_count(self, table: str) -> int:
            return spark.table(f"lakehouse.{settings.lake.bronze_namespace}.{table}").count()

        def bronze_columns(self, table: str) -> list[str]:
            return spark.table(f"lakehouse.{settings.lake.bronze_namespace}.{table}").columns

        def dead_letters(self) -> list[dict]:
            return [
                r.asDict()
                for r in spark.table(f"lakehouse.{settings.lake.bronze_namespace}._dead_letters").collect()
            ]

    return Pipeline()


def order(
    order_id: int, status: str = "placed", total: str = "100.00", note: str | None = "ring bell"
) -> dict:
    return {
        "order_id": order_id,
        "status": status,
        "total": total,
        "placed_at": "2026-09-01T10:15:30.123456Z",
        "note": note,
    }


def test_snapshot_then_streaming_changes(pipeline) -> None:
    pipeline.send(
        (ORDERS, ORDERS.record("r", order(1), 100, snapshot="true")),
        (ORDERS, ORDERS.record("r", order(2), 100, snapshot="last")),
    )
    pipeline.send(
        (ORDERS, ORDERS.record("u", order(1, "delivered", "120.50"), 200)),
        (ORDERS, ORDERS.record("d", order(2), 210)),
        (ORDERS, ORDERS.tombstone(order(2))),
        (ORDERS, ORDERS.record("c", order(3), 220)),
    )
    silver = pipeline.silver("orders")
    assert silver[(1,)]["status"] == "delivered"
    assert silver[(1,)]["total"] == Decimal("120.50")
    assert silver[(1,)]["placed_at"] == datetime(2026, 9, 1, 10, 15, 30, 123456)
    assert silver[(1,)]["_lsn"] == 200 and not silver[(1,)]["_is_deleted"]
    assert silver[(2,)]["_is_deleted"] and silver[(2,)]["_lsn"] == 210
    assert silver[(2,)]["status"] == "placed"  # tombstones keep the last known values
    assert not silver[(3,)]["_is_deleted"]
    assert pipeline.bronze_count("orders") == 5  # 2 snapshot reads + update + delete + insert


def test_duplicates_and_full_replay_change_nothing(pipeline) -> None:
    events = [
        (ORDERS, ORDERS.record("c", order(1), 100)),
        (ORDERS, ORDERS.record("u", order(1, "accepted"), 110)),
        (ORDERS, ORDERS.record("c", order(2), 120)),
        (ORDERS, ORDERS.record("d", order(2), 130)),
    ]
    pipeline.send(*events)
    before_silver, before_bronze = pipeline.silver("orders"), pipeline.bronze_count("orders")
    pipeline.send(*events)  # replay (new offsets: Debezium re-delivery after a restart looks like this)
    pipeline.send(events[1], events[1])  # duplicate inside a batch
    after = pipeline.silver("orders")
    strip = lambda rows: {k: {c: v for c, v in r.items() if c != "_ingested_at"} for k, r in rows.items()}  # noqa: E731
    assert strip(after) == strip(before_silver)
    assert pipeline.bronze_count("orders") == before_bronze == 4


def test_stale_events_do_not_overwrite_newer_state(pipeline) -> None:
    pipeline.send(
        (ORDERS, ORDERS.record("c", order(1), 100)), (ORDERS, ORDERS.record("u", order(1, "packed"), 300))
    )
    pipeline.send((ORDERS, ORDERS.record("u", order(1, "accepted"), 200)))  # arrives late
    assert pipeline.silver("orders")[(1,)]["status"] == "packed"
    assert pipeline.bronze_count("orders") == 3  # the late event is still part of the change history


def test_latest_change_in_a_batch_wins_regardless_of_arrival_order(pipeline) -> None:
    pipeline.send(
        (ORDERS, ORDERS.record("u", order(7, "packed"), 330)),
        (ORDERS, ORDERS.record("c", order(7), 310)),
        (ORDERS, ORDERS.record("u", order(7, "accepted"), 320)),
    )
    assert pipeline.silver("orders")[(7,)]["status"] == "packed"


def test_delete_before_insert_does_not_resurrect(pipeline) -> None:
    pipeline.send((ORDERS, ORDERS.record("d", order(5), 500)))
    pipeline.send((ORDERS, ORDERS.record("c", order(5), 450)))  # older insert delivered after the delete
    row = pipeline.silver("orders")[(5,)]
    assert row["_is_deleted"] and row["_lsn"] == 500


def test_reinsert_after_delete_with_composite_key(pipeline) -> None:
    key = {"store_id": 1, "product_id": 42}
    pipeline.send((INVENTORY, INVENTORY.record("c", {**key, "on_hand": 10}, 100)))
    pipeline.send(
        (INVENTORY, INVENTORY.record("d", {**key, "on_hand": None}, 200)),
        (INVENTORY, INVENTORY.tombstone(key)),
    )
    assert pipeline.silver("inventory")[(1, 42)]["_is_deleted"]
    pipeline.send((INVENTORY, INVENTORY.record("c", {**key, "on_hand": 55}, 300)))  # SKU relisted
    row = pipeline.silver("inventory")[(1, 42)]
    assert not row["_is_deleted"] and row["on_hand"] == 55


def test_new_source_column_evolves_bronze_and_silver(pipeline) -> None:
    pipeline.send((ORDERS, ORDERS.record("c", order(1), 100)))
    with_tip = ORDERS.with_columns(dbz.numeric("tip_amount", 8, 2))
    pipeline.send(
        (with_tip, with_tip.record("u", {**order(1, "delivered"), "tip_amount": "20.00"}, 200)),
        (with_tip, with_tip.record("c", {**order(2), "tip_amount": None}, 210)),
    )
    silver = pipeline.silver("orders")
    assert silver[(1,)]["tip_amount"] == Decimal("20.00")
    assert silver[(2,)]["tip_amount"] is None
    assert "tip_amount" in pipeline.bronze_columns("orders")
    assert pipeline.metrics.schema_changes >= 2  # bronze + silver


def test_mixed_schema_versions_in_one_batch(pipeline) -> None:
    with_tip = ORDERS.with_columns(dbz.numeric("tip_amount", 8, 2))
    pipeline.send(
        (ORDERS, ORDERS.record("c", order(1), 100)),
        (with_tip, with_tip.record("u", {**order(1, "delivered"), "tip_amount": "30.00"}, 200)),
    )
    assert pipeline.silver("orders")[(1,)]["tip_amount"] == Decimal("30.00")


def test_toasted_value_placeholder_keeps_previous_value(pipeline) -> None:
    pipeline.send((ORDERS, ORDERS.record("c", order(1, note="leave at door"), 100)))
    pipeline.send((ORDERS, ORDERS.record("u", order(1, "accepted", note=TOAST_PLACEHOLDER), 200)))
    row = pipeline.silver("orders")[(1,)]
    assert row["status"] == "accepted" and row["note"] == "leave at door"


def test_truncate_tombstones_existing_rows(pipeline) -> None:
    pipeline.send((ORDERS, ORDERS.record("c", order(1), 100)), (ORDERS, ORDERS.record("c", order(2), 110)))
    truncate = {
        "schema": ORDERS.envelope_schema(),
        "payload": {
            "before": None,
            "after": None,
            "op": "t",
            "ts_ms": 1,
            "source": {
                "version": "3.7.0.Final",
                "connector": "postgresql",
                "name": "qc",
                "ts_ms": 1,
                "snapshot": "false",
                "db": "qcommerce",
                "schema": "commerce",
                "table": "orders",
                "txId": 20,
                "lsn": 200,
            },
        },
    }
    pipeline.send((ORDERS, (None, json.dumps(truncate))), (ORDERS, ORDERS.record("c", order(3), 210)))
    silver = pipeline.silver("orders")
    assert silver[(1,)]["_is_deleted"] and silver[(2,)]["_is_deleted"]
    assert not silver[(3,)]["_is_deleted"]


def test_malformed_records_are_dead_lettered(pipeline) -> None:
    pipeline.send(
        (ORDERS, ('{"payload": {"order_id": 9}}', "this is not json")),
        (ORDERS, ORDERS.record("c", order(1), 100)),
    )
    assert pipeline.silver("orders")[(1,)]["status"] == "placed"
    dead = pipeline.dead_letters()
    assert len(dead) == 1 and dead[0]["reason"] == "unparseable_envelope"
    pipeline.send((ORDERS, ('{"payload": {"order_id": 9}}', "this is not json")))
    assert len(pipeline.dead_letters()) == 2  # a different offset is a different record


def test_ingested_timestamps_are_utc(pipeline) -> None:
    pipeline.send(
        (ORDERS, ORDERS.record("c", order(1), 100, ts=datetime(2026, 9, 2, 8, 0, tzinfo=timezone.utc)))
    )
    row = pipeline.silver("orders")[(1,)]
    assert row["_source_ts"] == datetime(2026, 9, 2, 8, 0)
