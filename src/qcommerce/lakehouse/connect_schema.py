"""Kafka Connect (Debezium) JSON schemas -> Spark types and decoding expressions.

With ``value.converter.schemas.enable=true`` every message embeds the Connect schema of its envelope.
The writer reads the distinct schemas present in a micro-batch, merges them (a new column appears as
soon as the first event carrying it arrives), parses the payload with the *raw* JSON types and then
decodes Debezium's logical types into proper column types:

=========================================  ====================  =====================
Connect type / logical name                JSON encoding         Spark / Iceberg type
=========================================  ====================  =====================
int8, int16, int32                         number                int
int64                                      number                bigint
float / double                             number                float / double
boolean / string                           as-is                 boolean / string
string + source type NUMERIC/DECIMAL(p,s)  "123.45"              decimal(p,s)
io.debezium.time.ZonedTimestamp            ISO-8601 string       timestamp
io.debezium.time.Timestamp / Micro / Nano  epoch ms / us / ns    timestamp
io.debezium.time.Date                      days since epoch      date
io.debezium.data.Json / Uuid / Enum        string                string
bytes                                      base64 string         binary
=========================================  ====================  =====================

(The NUMERIC precision comes from ``column.propagate.source.type`` together with
``decimal.handling.mode=string``.)
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import Column

TOAST_PLACEHOLDER = "__debezium_unavailable_value"

_TIMESTAMP_LOGICAL = {
    "io.debezium.time.ZonedTimestamp": "iso",
    "io.debezium.time.Timestamp": "millis",
    "org.apache.kafka.connect.data.Timestamp": "millis",
    "io.debezium.time.MicroTimestamp": "micros",
    "io.debezium.time.NanoTimestamp": "nanos",
}
_DATE_LOGICAL = {"io.debezium.time.Date", "org.apache.kafka.connect.data.Date"}
_UNSUPPORTED_LOGICAL = {"org.apache.kafka.connect.data.Decimal", "io.debezium.data.VariableScaleDecimal"}
_NUMERIC_SOURCE_TYPES = {"NUMERIC", "DECIMAL"}
# The JSON converter names them "float"/"double"; the Connect API calls them FLOAT32/FLOAT64.
FLOAT_TYPES = frozenset({"float", "float32"})
DOUBLE_TYPES = frozenset({"double", "float64"})


class UnsupportedSchema(ValueError):
    pass


@dataclass(frozen=True)
class ConnectField:
    name: str
    type: str
    optional: bool = True
    logical: str | None = None
    parameters: tuple[tuple[str, str], ...] = ()
    fields: tuple[ConnectField, ...] = ()
    items: ConnectField | None = None
    values: ConnectField | None = None

    @property
    def params(self) -> dict[str, str]:
        return dict(self.parameters)

    @property
    def source_type(self) -> str | None:
        return self.params.get("__debezium.source.column.type")


@dataclass(frozen=True)
class TableSchema:
    """Business columns of one table (in source order) and its primary key."""

    columns: tuple[ConnectField, ...]
    primary_key: tuple[str, ...]

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def non_key(self) -> list[ConnectField]:
        return [c for c in self.columns if c.name not in self.primary_key]


def parse_field(obj: dict[str, Any], name: str | None = None) -> ConnectField:
    kind = obj["type"]
    return ConnectField(
        name=name if name is not None else obj.get("field", ""),
        type=kind,
        optional=bool(obj.get("optional", True)),
        logical=obj.get("name"),
        parameters=tuple(sorted((obj.get("parameters") or {}).items())),
        fields=tuple(parse_field(f) for f in obj.get("fields", ())) if kind == "struct" else (),
        items=parse_field(obj["items"], "element") if kind == "array" else None,
        values=parse_field(obj["values"], "value") if kind == "map" else None,
    )


def envelope_columns(envelope: dict[str, Any]) -> tuple[ConnectField, ...]:
    """The row columns of a Debezium envelope schema (the ``after`` struct)."""
    for f in envelope.get("fields", ()):
        if f.get("field") in ("after", "before") and f.get("type") == "struct":
            return parse_field(f).fields
    raise UnsupportedSchema(f"not a Debezium envelope: {envelope.get('name')}")


def key_columns(key_schema: dict[str, Any]) -> tuple[ConnectField, ...]:
    if key_schema.get("type") != "struct":
        raise UnsupportedSchema("message keys must be structs (tables need a primary key)")
    return parse_field(key_schema).fields


def merge_columns(versions: Iterable[tuple[ConnectField, ...]]) -> tuple[ConnectField, ...]:
    """Union of columns across schema versions, keeping first-seen order (new columns go last)."""
    merged: dict[str, ConnectField] = {}
    for columns in versions:
        for col in columns:
            current = merged.get(col.name)
            if current is None:
                merged[col.name] = col
            elif decoded_type(current) != decoded_type(col):
                widened = _widen(decoded_type(current), decoded_type(col))
                if widened is None:
                    raise UnsupportedSchema(
                        f"column {col.name!r} changed type incompatibly: {decoded_type(current)} -> {decoded_type(col)}"
                    )
                merged[col.name] = col if widened == decoded_type(col) else current
    return tuple(merged.values())


def raw_type(f: ConnectField) -> DataType:
    """How the value is encoded in the JSON payload."""
    if f.type in ("int8", "int16", "int32"):
        return IntegerType()
    if f.type == "int64":
        return LongType()
    if f.type in FLOAT_TYPES | DOUBLE_TYPES:
        return DoubleType()
    if f.type == "boolean":
        return BooleanType()
    if f.type in ("string", "bytes"):
        return StringType()
    if f.type == "struct":
        return StructType([StructField(c.name, raw_type(c), True) for c in f.fields])
    if f.type == "array" and f.items is not None:
        return ArrayType(raw_type(f.items), True)
    if f.type == "map" and f.values is not None:
        return MapType(StringType(), raw_type(f.values), True)
    raise UnsupportedSchema(f"unsupported Connect type {f.type!r} for column {f.name!r}")


def decoded_type(f: ConnectField) -> DataType:
    """The column type in bronze/silver."""
    if f.logical in _UNSUPPORTED_LOGICAL:
        raise UnsupportedSchema(f"{f.logical} on {f.name!r}: configure decimal.handling.mode=string")
    if f.logical in _TIMESTAMP_LOGICAL:
        return TimestampType()
    if f.logical in _DATE_LOGICAL:
        return DateType()
    if f.type == "string" and (f.source_type or "").upper() in _NUMERIC_SOURCE_TYPES:
        precision = int(f.params.get("__debezium.source.column.length", "0") or 0)
        scale = int(f.params.get("__debezium.source.column.scale", "0") or 0)
        if 0 < precision <= 38 and 0 <= scale <= precision:
            return DecimalType(precision, scale)
        return DecimalType(38, 10)  # unconstrained numeric
    if f.type in FLOAT_TYPES:
        return FloatType()
    if f.type == "bytes":
        return BinaryType()
    if f.type == "struct":
        return StructType([StructField(c.name, decoded_type(c), True) for c in f.fields])
    if f.type == "array" and f.items is not None:
        return ArrayType(decoded_type(f.items), True)
    if f.type == "map" and f.values is not None:
        return MapType(StringType(), decoded_type(f.values), True)
    return raw_type(f)


def decode(column: Column, f: ConnectField) -> Column:
    """Expression converting the raw JSON value of ``f`` into :func:`decoded_type`."""
    from pyspark.sql import functions as F

    target = decoded_type(f)
    encoding = _TIMESTAMP_LOGICAL.get(f.logical or "")
    if encoding == "iso":
        return column.cast(TimestampType())
    if encoding == "millis":
        return F.timestamp_millis(column)
    if encoding == "micros":
        return F.timestamp_micros(column)
    if encoding == "nanos":
        return F.timestamp_micros(F.floor(column / 1000).cast(LongType()))
    if f.logical in _DATE_LOGICAL:
        return F.date_from_unix_date(column)
    if f.type == "bytes":
        return F.unbase64(column)
    if f.type in ("struct", "array", "map"):
        return column.cast(target)
    return column if target == raw_type(f) else column.cast(target)


def _widen(a: DataType, b: DataType) -> DataType | None:
    """Iceberg-legal type promotions: int->long, float->double, decimal(p,s)->decimal(p',s) with p'>p."""
    pairs = {(IntegerType(), LongType()): LongType(), (FloatType(), DoubleType()): DoubleType()}
    for (x, y), out in pairs.items():
        if (a, b) in ((x, y), (y, x)):
            return out
    if isinstance(a, DecimalType) and isinstance(b, DecimalType) and a.scale == b.scale:
        return a if a.precision >= b.precision else b
    return None


def widen(a: DataType, b: DataType) -> DataType | None:
    return a if a == b else _widen(a, b)
