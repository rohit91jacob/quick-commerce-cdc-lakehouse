"""Applies simulator transactions to Postgres, and the pause/resume control channel."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import Any

import psycopg
from psycopg import sql

from qcommerce import log
from qcommerce.generator.ops import Ddl, Delete, Insert, Op, Txn, Update

logger = log.get(__name__)

SCHEMA = "commerce"


class RowCountMismatch(RuntimeError):
    """An UPDATE/DELETE did not hit exactly one row: simulator state and database have diverged."""


class PostgresSink:
    def __init__(
        self, conn: psycopg.Connection, *, txn_batch: int = 1, migrate: Callable[[int], None] | None = None
    ) -> None:
        self.conn = conn
        self.txn_batch = max(1, txn_batch)
        self._migrate = migrate
        self._pending = 0
        self._cache: dict[tuple[Any, ...], sql.Composed] = {}
        self.applied_ops = 0
        self.applied_txns = 0

    # SQL builders are cached per (kind, table, columns) shape.
    def _insert_sql(self, table: str, columns: tuple[str, ...]) -> sql.Composed:
        key = ("i", table, columns)
        if key not in self._cache:
            self._cache[key] = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(SCHEMA),
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            )
        return self._cache[key]

    def _update_sql(self, table: str, columns: tuple[str, ...], keys: tuple[str, ...]) -> sql.Composed:
        cache_key = ("u", table, columns, keys)
        if cache_key not in self._cache:
            self._cache[cache_key] = sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
                sql.Identifier(SCHEMA),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in columns),
                sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in keys),
            )
        return self._cache[cache_key]

    def _delete_sql(self, table: str, keys: tuple[str, ...]) -> sql.Composed:
        cache_key = ("d", table, keys)
        if cache_key not in self._cache:
            self._cache[cache_key] = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                sql.Identifier(SCHEMA),
                sql.Identifier(table),
                sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in keys),
            )
        return self._cache[cache_key]

    def apply(self, txn: Txn) -> None:
        ddl = [op for op in txn.ops if isinstance(op, Ddl)]
        if ddl:
            self.flush()
            if self._migrate is None:
                raise RuntimeError("schema change requested but no migration callback configured")
            for op in ddl:
                self._migrate(op.migration_version)
            logger.info("schema change applied", extra={"migration": [op.migration_version for op in ddl]})
            self.applied_txns += 1
            return
        with self.conn.cursor() as cur:
            for group in _group_inserts(txn.ops):
                if isinstance(group, list):  # consecutive inserts into the same table with the same columns
                    columns = tuple(group[0].row)
                    cur.executemany(
                        self._insert_sql(group[0].table, columns), [tuple(i.row.values()) for i in group]
                    )
                elif isinstance(group, Update):
                    columns, keys = tuple(group.values), tuple(group.key)
                    cur.execute(
                        self._update_sql(group.table, columns, keys),
                        (*group.values.values(), *group.key.values()),
                    )
                    if cur.rowcount != 1:
                        raise RowCountMismatch(f"UPDATE {group.table} {group.key} hit {cur.rowcount} rows")
                elif isinstance(group, Delete):
                    keys = tuple(group.key)
                    cur.execute(self._delete_sql(group.table, keys), tuple(group.key.values()))
                    if cur.rowcount != 1:
                        raise RowCountMismatch(f"DELETE {group.table} {group.key} hit {cur.rowcount} rows")
        self.applied_ops += len(txn.ops)
        self.applied_txns += 1
        self._pending += 1
        if self._pending >= self.txn_batch:
            self.flush()

    def flush(self) -> None:
        if self._pending:
            self.conn.commit()
            self._pending = 0

    def apply_all(self, txns: Iterable[Txn]) -> None:
        for txn in txns:
            self.apply(txn)
        self.flush()


def _group_inserts(ops: list[Op]) -> list[Any]:
    groups: list[Any] = []
    for op in ops:
        if (
            isinstance(op, Insert)
            and groups
            and isinstance(groups[-1], list)
            and groups[-1][0].table == op.table
            and tuple(groups[-1][0].row) == tuple(op.row)
        ):
            groups[-1].append(op)
        elif isinstance(op, Insert):
            groups.append([op])
        else:
            groups.append(op)
    return groups


class GeneratorControl:
    """``ops.generator_control``: lets other jobs pause the workload (e.g. exact reconciliation)."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        self._last_poll = 0.0
        self._paused = False

    def poll(self, *, every_s: float = 1.0) -> bool:
        """Return True while paused. Acknowledges a pause request and records a heartbeat."""
        now = time.monotonic()
        if now - self._last_poll < every_s:
            return self._paused
        self._last_poll = now
        row = self.conn.execute(
            "UPDATE ops.generator_control SET heartbeat_at = now(), "
            "acknowledged_at = CASE WHEN paused THEN coalesce(acknowledged_at, now()) END "
            "WHERE id = 1 RETURNING paused"
        ).fetchone()
        self.conn.commit()
        paused = bool(row and row[0])
        if paused != self._paused:
            logger.info("generator %s", "paused" if paused else "resumed")
        self._paused = paused
        return paused

    @staticmethod
    def request(conn: psycopg.Connection, *, paused: bool, by: str) -> None:
        conn.execute(
            "UPDATE ops.generator_control SET paused = %s, requested_by = %s, requested_at = now(), "
            "acknowledged_at = NULL WHERE id = 1",
            (paused, by),
        )
        conn.commit()

    @staticmethod
    def wait_acknowledged(conn: psycopg.Connection, *, timeout_s: float = 60.0) -> bool:
        """Wait for a running generator to confirm the pause; True if acknowledged (or none running)."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            row = conn.execute(
                "SELECT acknowledged_at IS NOT NULL, heartbeat_at > now() - interval '10 seconds' "
                "FROM ops.generator_control WHERE id = 1"
            ).fetchone()
            conn.commit()
            acknowledged, alive = row if row else (True, False)
            if acknowledged or not alive:
                return True
            time.sleep(0.5)
        return False
