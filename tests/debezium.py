"""Builders for Debezium-format Kafka records (JSON converter with embedded schemas), as the
Postgres connector emits them with this project's connector settings."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

SOURCE_SCHEMA = {
    "type": "struct",
    "optional": False,
    "name": "io.debezium.connector.postgresql.Source",
    "field": "source",
    "fields": [
        {"type": "string", "optional": False, "field": "version"},
        {"type": "string", "optional": False, "field": "connector"},
        {"type": "string", "optional": False, "field": "name"},
        {"type": "int64", "optional": False, "field": "ts_ms"},
        {"type": "string", "optional": True, "name": "io.debezium.data.Enum", "field": "snapshot"},
        {"type": "string", "optional": False, "field": "db"},
        {"type": "string", "optional": True, "field": "sequence"},
        {"type": "string", "optional": False, "field": "schema"},
        {"type": "string", "optional": False, "field": "table"},
        {"type": "int64", "optional": True, "field": "txId"},
        {"type": "int64", "optional": True, "field": "lsn"},
        {"type": "int64", "optional": True, "field": "xmin"},
    ],
}


def int64(name: str, optional: bool = False) -> dict[str, Any]:
    return {"type": "int64", "optional": optional, "field": name}


def int32(name: str, optional: bool = False) -> dict[str, Any]:
    return {"type": "int32", "optional": optional, "field": name}


def text(name: str, optional: bool = True) -> dict[str, Any]:
    return {
        "type": "string",
        "optional": optional,
        "field": name,
        "parameters": {
            "__debezium.source.column.type": "TEXT",
            "__debezium.source.column.length": "2147483647",
            "__debezium.source.column.scale": "0",
        },
    }


def numeric(name: str, precision: int, scale: int) -> dict[str, Any]:
    return {
        "type": "string",
        "optional": True,
        "field": name,
        "parameters": {
            "__debezium.source.column.type": "NUMERIC",
            "__debezium.source.column.length": str(precision),
            "__debezium.source.column.scale": str(scale),
        },
    }


def timestamptz(name: str) -> dict[str, Any]:
    return {
        "type": "string",
        "optional": True,
        "name": "io.debezium.time.ZonedTimestamp",
        "version": 1,
        "field": name,
    }


def boolean(name: str) -> dict[str, Any]:
    return {"type": "boolean", "optional": True, "field": name}


def double(name: str) -> dict[str, Any]:
    return {"type": "double", "optional": True, "field": name}


class Table:
    def __init__(self, name: str, key: list[str], columns: list[dict[str, Any]]) -> None:
        self.name = name
        self.key = key
        self.columns = columns
        self.topic = f"qc.commerce.{name}"

    def with_columns(self, *extra: dict[str, Any]) -> Table:
        return Table(self.name, self.key, [*self.columns, *extra])

    def _row_schema(self, field: str) -> dict[str, Any]:
        return {
            "type": "struct",
            "fields": self.columns,
            "optional": True,
            "name": f"qc.commerce.{self.name}.Value",
            "field": field,
        }

    def envelope_schema(self) -> dict[str, Any]:
        return {
            "type": "struct",
            "optional": False,
            "name": f"qc.commerce.{self.name}.Envelope",
            "version": 2,
            "fields": [
                self._row_schema("before"),
                self._row_schema("after"),
                SOURCE_SCHEMA,
                {"type": "string", "optional": False, "field": "op"},
                {"type": "int64", "optional": True, "field": "ts_ms"},
            ],
        }

    def key_schema(self) -> dict[str, Any]:
        fields = [c for c in self.columns if c["field"] in self.key]
        return {"type": "struct", "optional": False, "name": f"qc.commerce.{self.name}.Key", "fields": fields}

    def record(
        self, op: str, row: dict[str, Any], lsn: int, *, ts: datetime | None = None, snapshot: str = "false"
    ) -> tuple[str, str | None]:
        """(key_json, value_json) for a change event; op in c/u/d/r."""
        ts_ms = int((ts or datetime(2026, 9, 1, tzinfo=timezone.utc)).timestamp() * 1000)
        key = {k: row[k] for k in self.key}
        after = None if op == "d" else row
        before = key if op == "d" else None
        value = {
            "schema": self.envelope_schema(),
            "payload": {
                "before": before,
                "after": after,
                "source": {
                    "version": "3.7.0.Final",
                    "connector": "postgresql",
                    "name": "qc",
                    "ts_ms": ts_ms,
                    "snapshot": snapshot,
                    "db": "qcommerce",
                    "sequence": None,
                    "schema": "commerce",
                    "table": self.name,
                    "txId": lsn // 10,
                    "lsn": lsn,
                    "xmin": None,
                },
                "op": op,
                "ts_ms": ts_ms + 5,
            },
        }
        return json.dumps({"schema": self.key_schema(), "payload": key}), json.dumps(value)

    def tombstone(self, row: dict[str, Any]) -> tuple[str, None]:
        key = {k: row[k] for k in self.key}
        return json.dumps({"schema": self.key_schema(), "payload": key}), None
