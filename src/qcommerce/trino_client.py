"""Thin helpers around the Trino DB-API client."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

import trino

from qcommerce.settings import TrinoSettings


def connect(settings: TrinoSettings, schema: str | None = None) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        catalog=settings.catalog,
        schema=schema,
        http_scheme="http",
        request_timeout=600,
    )


def query(conn: trino.dbapi.Connection, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()


def wait_ready(settings: TrinoSettings, timeout_s: float = 300.0) -> None:
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            query(connect(settings), "SELECT 1")
            return
        except Exception as exc:  # starting up: connection refused / SERVER_STARTING_UP
            last = exc
            time.sleep(3)
    raise TimeoutError(f"Trino not ready after {timeout_s}s: {last}")


def table_exists(conn: trino.dbapi.Connection, schema: str, table: str) -> bool:
    rows = query(
        conn,
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    )
    return bool(rows and rows[0][0])
