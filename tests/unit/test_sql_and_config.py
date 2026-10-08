from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from qcommerce import reconcile
from qcommerce.cdc.connector import connector_config, is_running
from qcommerce.cli import build_parser
from qcommerce.lakehouse import iceberg
from qcommerce.lakehouse.metrics import WriterMetrics
from qcommerce.settings import Settings


def test_silver_merge_is_lsn_guarded_and_tombstones_deletes() -> None:
    sql = iceberg.silver_merge_sql(
        "cat.silver.orders", "src", ["order_id"], ["order_id", "status", "total"], ["status"]
    )
    assert "WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN UPDATE SET" in sql
    assert "t._is_deleted = true" in sql
    assert "WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET" in sql
    assert "s._op = 'd'" in sql.split("VALUES")[1]  # an unseen delete is inserted as a tombstone
    assert f"CASE WHEN s.`status` = '{iceberg.TOAST}' THEN t.`status`" in sql
    assert "CASE WHEN s.`total`" not in sql  # placeholder handling only for strings
    assert "t.`order_id` =" in sql and "t.`order_id` = s.`order_id`," not in sql.split("WHEN MATCHED")[2]


def test_bronze_merge_dedupes_on_key_lsn_op_and_only_inserts() -> None:
    sql = iceberg.bronze_merge_sql(
        "cat.bronze.inventory",
        "src",
        ["store_id", "product_id"],
        ["store_id", "product_id", "_lsn", "_op"],
        42,
    )
    assert "t.`store_id` = s.`store_id` AND t.`product_id` = s.`product_id`" in sql
    assert "t._lsn = s._lsn AND t._op = s._op AND t._lsn >= 42" in sql
    assert "WHEN MATCHED" not in sql and "WHEN NOT MATCHED THEN INSERT" in sql


def test_identifiers_are_quoted() -> None:
    assert iceberg.q("we`ird") == "`we``ird`"


def test_connector_config_essentials() -> None:
    config = connector_config(Settings())
    assert config["plugin.name"] == "pgoutput"
    assert config["publication.autocreate.mode"] == "disabled"
    assert config["decimal.handling.mode"] == "string"
    assert config["value.converter.schemas.enable"] == "true"
    assert "heartbeat.action.query" in config
    assert config["signal.data.collection"].endswith(".debezium_signal")


def test_is_running() -> None:
    running = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
    assert is_running(running)
    assert not is_running({"connector": {"state": "RUNNING"}, "tasks": [{"state": "FAILED"}]})
    assert not is_running(None)


def test_reconcile_comparisons() -> None:
    assert reconcile._same("total:sum", Decimal("10.50"), Decimal("10.5"))
    assert reconcile._same("qty:sum", 7, Decimal(7))
    assert not reconcile._same("qty:sum", 7, 8)
    assert reconcile._same("lat:sum~", 1234.5678901, 1234.5678902)
    assert not reconcile._same("lat:sum~", 1234.5, 1234.6)
    assert reconcile._same("x:sum", None, 0)
    ts = datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert reconcile._same("t:max", ts, ts)


def test_reconcile_aggregates_by_type() -> None:
    labels = [label for label, _ in reconcile._aggregates("x", "numeric", "pg")]
    assert labels == ["x:count", "x:sum"]
    assert [lbl for lbl, _ in reconcile._aggregates("t", "timestamp with time zone", "pg")] == [
        "t:count",
        "t:min",
        "t:max",
    ]
    assert [lbl for lbl, _ in reconcile._aggregates("s", "text", "pg")] == ["s:count", "s:length"]


def test_metrics_exposition() -> None:
    from collections import Counter

    metrics = WriterMetrics()
    metrics.record_batch(3, 1.5, Counter({("orders", "u"): 4}), 0, 1)
    metrics.record_progress(
        {"sources": [{"metrics": {"maxOffsetsBehindLatest": "12", "avgOffsetsBehindLatest": "3"}}]}
    )
    text = metrics.prometheus()
    assert "qc_writer_last_batch_id 3" in text
    assert 'qc_writer_events_total{table="orders",op="u"} 4' in text
    assert 'qc_writer_offsets_behind_latest{stat="max"} 12.0' in text


def test_cli_parses_every_command() -> None:
    parser = build_parser()
    for argv in (
        ["db", "migrate", "--target", "1"],
        ["generator", "run", "--backfill-hours", "2"],
        ["cdc", "snapshot", "--tables", "commerce.orders"],
        ["writer", "run", "--available-now"],
        ["reconcile", "--mode", "settled"],
        ["maintenance", "run"],
        ["ops", "status"],
    ):
        assert parser.parse_args(argv).func is not None
