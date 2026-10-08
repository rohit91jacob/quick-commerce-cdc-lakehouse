from __future__ import annotations

import pytest
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from qcommerce.lakehouse import connect_schema as cs
from tests import debezium as dbz


def field(obj: dict) -> cs.ConnectField:
    return cs.parse_field(obj)


@pytest.mark.parametrize(
    ("obj", "expected"),
    [
        (dbz.int32("store_id"), IntegerType()),
        ({"type": "int16", "field": "x"}, IntegerType()),  # Iceberg has no smallint
        (dbz.int64("order_id"), LongType()),
        (dbz.double("lat"), DoubleType()),
        (dbz.text("status"), StringType()),
        (dbz.numeric("total", 10, 2), DecimalType(10, 2)),
        (dbz.timestamptz("placed_at"), TimestampType()),
        ({"type": "int32", "name": "io.debezium.time.Date", "field": "d"}, DateType()),
        ({"type": "int64", "name": "io.debezium.time.MicroTimestamp", "field": "t"}, TimestampType()),
        ({"type": "string", "name": "io.debezium.data.Json", "field": "j"}, StringType()),
    ],
)
def test_decoded_types(obj: dict, expected) -> None:
    assert cs.decoded_type(field(obj)) == expected


def test_unconstrained_numeric_falls_back_to_wide_decimal() -> None:
    f = field({"type": "string", "field": "n", "parameters": {"__debezium.source.column.type": "NUMERIC"}})
    assert cs.decoded_type(f) == DecimalType(38, 10)


def test_precise_decimal_mode_is_rejected_with_a_hint() -> None:
    f = field({"type": "bytes", "name": "org.apache.kafka.connect.data.Decimal", "field": "n"})
    with pytest.raises(cs.UnsupportedSchema, match="decimal.handling.mode=string"):
        cs.decoded_type(f)


def test_envelope_and_key_columns() -> None:
    table = dbz.Table("orders", ["order_id"], [dbz.int64("order_id"), dbz.text("status")])
    assert [c.name for c in cs.envelope_columns(table.envelope_schema())] == ["order_id", "status"]
    assert [c.name for c in cs.key_columns(table.key_schema())] == ["order_id"]


def test_merge_keeps_order_and_appends_new_columns() -> None:
    v1 = (field(dbz.int64("order_id")), field(dbz.text("status")))
    v2 = (*v1, field(dbz.numeric("tip_amount", 8, 2)))
    merged = cs.merge_columns([v2, v1])
    assert [c.name for c in merged] == ["order_id", "status", "tip_amount"]


def test_merge_widens_int_to_long() -> None:
    merged = cs.merge_columns([(field(dbz.int32("qty")),), (field(dbz.int64("qty")),)])
    assert cs.decoded_type(merged[0]) == LongType()


def test_merge_rejects_incompatible_type_change() -> None:
    with pytest.raises(cs.UnsupportedSchema):
        cs.merge_columns([(field(dbz.int64("x")),), (field(dbz.text("x")),)])


def test_widen_rules() -> None:
    assert cs.widen(DecimalType(8, 2), DecimalType(10, 2)) == DecimalType(10, 2)
    assert cs.widen(DecimalType(8, 2), DecimalType(10, 3)) is None
    assert cs.widen(StringType(), StringType()) == StringType()
