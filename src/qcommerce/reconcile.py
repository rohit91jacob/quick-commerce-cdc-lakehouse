"""Source-vs-lakehouse reconciliation: the end-to-end correctness check of the CDC pipeline.

For every business table, the same column profile is computed on Postgres and on the Iceberg silver
table (live rows only): row count, and per column the non-null count plus an exact aggregate (sum for
integers and decimals, total length for text, true-count for booleans, min/max for timestamps; doubles
are compared with a 1e-6 relative tolerance). Any difference is a mismatch.

Modes
  exact    Pause the generator, then prove the pipeline has caught up with a two-phase fence:
           fence 1 is committed and Debezium's slot must confirm it (so every earlier change is durably
           in Kafka); fence 2 is committed and must become visible in silver (the micro-batch that
           carried it was planned after everything up to fence 1 was in Kafka). Then compare everything.
  settled  Leave the workload running and compare only rows last updated before ``now - settle``,
           after a single fence shows the writer is current. Assumes ``updated_at`` tracks commit time,
           which holds for OLTP traffic (but not during a synthetic backfill).
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg

from qcommerce import log, trino_client
from qcommerce.generator.sink import GeneratorControl
from qcommerce.oltp import BUSINESS_TABLES
from qcommerce.settings import Settings

logger = log.get(__name__)

FENCE_ID = 2
INTEGER_TYPES = {"smallint", "integer", "bigint"}


@dataclass
class TableResult:
    table: str
    ok: bool
    rows_source: int
    rows_lakehouse: int
    mismatches: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class Report:
    mode: str
    ok: bool
    started_at: str
    cut: str | None
    fence_lsn: str | None
    tables: list[TableResult]
    seconds: float

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


def _aggregates(column: str, data_type: str, dialect: str) -> list[tuple[str, str]]:
    """[(label, sql expression)] for one column."""
    c = f'"{column}"'
    out = [(f"{column}:count", f"count({c})")]
    if data_type in INTEGER_TYPES or data_type == "numeric":
        out.append((f"{column}:sum", f"sum({c})"))
    elif data_type == "double precision":
        out.append((f"{column}:sum~", f"sum({c})"))
    elif data_type in ("text", "character varying"):
        out.append((f"{column}:length", f"sum(length({c}))"))
    elif data_type == "boolean":
        out.append((f"{column}:true", f"sum(CASE WHEN {c} THEN 1 ELSE 0 END)"))
    elif data_type.startswith("timestamp") or data_type == "date":
        out += [(f"{column}:min", f"min({c})"), (f"{column}:max", f"max({c})")]
    return out


def _source_columns(conn: psycopg.Connection, schema: str, table: str) -> list[tuple[str, str]]:
    return conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    ).fetchall()


def _lake_columns(tconn, schema: str, table: str) -> set[str]:
    rows = trino_client.query(
        tconn,
        "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    )
    return {r[0] for r in rows}


def _same(label: str, a: Any, b: Any) -> bool:
    if a is None or b is None:
        return (a or 0) == (b or 0)
    if label.endswith("~"):
        return abs(float(a) - float(b)) <= 1e-6 * max(1.0, abs(float(a)))
    if isinstance(a, datetime) and isinstance(b, datetime):
        return a == b
    if isinstance(a, (int, Decimal)) and isinstance(b, (int, Decimal)):
        return Decimal(a) == Decimal(b)
    return a == b


def compare_table(
    pg: psycopg.Connection, tconn, settings: Settings, table: str, cut: datetime | None
) -> TableResult:
    schema = settings.pg.app_schema
    silver = settings.lake.silver_namespace
    columns = _source_columns(pg, schema, table)
    lake_cols = _lake_columns(tconn, silver, table)
    result = TableResult(table, True, 0, 0)
    if not lake_cols:
        result.ok = False
        result.mismatches.append("silver table does not exist")
        return result
    shared = [(c, t) for c, t in columns if c in lake_cols]
    for c, _ in columns:
        if c not in lake_cols:
            result.ok = False
            result.mismatches.append(f"column {c} missing in silver")
    for c in sorted(lake_cols - {c for c, _ in columns}):
        if not c.startswith("_"):
            result.warnings.append(f"column {c} exists only in silver (dropped at the source?)")

    labels: list[str] = ["rows"]
    pg_exprs, lake_exprs = ["count(*)"], ["count(*)"]
    for c, t in shared:
        for label, expr in _aggregates(c, t, "pg"):
            labels.append(label)
            pg_exprs.append(expr)
            lake_exprs.append(expr)
    pg_where = "WHERE updated_at <= %s" if cut else ""
    lake_where = "WHERE NOT _is_deleted" + (" AND updated_at <= from_iso8601_timestamp(?)" if cut else "")
    pg_row = pg.execute(
        f'SELECT {", ".join(pg_exprs)} FROM {schema}."{table}" {pg_where}', (cut,) if cut else None
    ).fetchone()
    lake_row = trino_client.query(
        tconn,
        f'SELECT {", ".join(lake_exprs)} FROM {silver}."{table}" {lake_where}',
        [cut.isoformat()] if cut else None,
    )[0]
    result.rows_source, result.rows_lakehouse = int(pg_row[0]), int(lake_row[0])
    for label, a, b in zip(labels, pg_row, lake_row, strict=True):
        if not _same(label, a, b):
            result.ok = False
            result.mismatches.append(f"{label}: source={a} lakehouse={b}")
    return result


# ------------------------------------------------------------------------------------------- fences


def _write_fence(conn: psycopg.Connection, settings: Settings) -> tuple[str, str]:
    token = uuid.uuid4().hex
    conn.execute(
        f"UPDATE {settings.pg.app_schema}.cdc_heartbeat SET token = %s, beat_at = now(), updated_at = now() "
        "WHERE id = %s",
        (token, FENCE_ID),
    )
    conn.commit()
    lsn = conn.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    conn.commit()
    return token, lsn


def _wait_slot_confirms(conn: psycopg.Connection, settings: Settings, lsn: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        row = conn.execute(
            "SELECT confirmed_flush_lsn >= %s::pg_lsn FROM pg_replication_slots WHERE slot_name = %s",
            (lsn, settings.pg.slot),
        ).fetchone()
        conn.commit()
        if row and row[0]:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"replication slot {settings.pg.slot} did not confirm {lsn} within {timeout_s}s"
            )
        time.sleep(1)


def _wait_silver_sees(tconn, settings: Settings, token: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    sql = f"SELECT token FROM {settings.lake.silver_namespace}.cdc_heartbeat WHERE id = {FENCE_ID}"
    while True:
        try:
            rows = trino_client.query(tconn, sql)
            if rows and rows[0][0] == token:
                return
        except Exception as exc:  # the table appears with the first batch
            logger.debug("fence table not readable yet", extra={"error": str(exc)})
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"fence {token} not visible in silver within {timeout_s}s (is the writer running?)"
            )
        time.sleep(2)


# ------------------------------------------------------------------------------------------- entry


def run(
    settings: Settings,
    *,
    mode: str = "exact",
    settle_minutes: float = 15.0,
    timeout_s: float = 900.0,
    pause_generator: bool = True,
    attempts: int = 3,
) -> Report:
    if mode not in ("exact", "settled"):
        raise ValueError(f"unknown mode {mode}")
    started = time.monotonic()
    started_at = datetime.now(timezone.utc)
    tconn = trino_client.connect(settings.trino, schema=settings.lake.silver_namespace)
    with psycopg.connect(settings.pg.dsn()) as pg:
        paused = False
        try:
            if mode == "exact" and pause_generator:
                GeneratorControl.request(pg, paused=True, by="reconcile")
                paused = True
                if not GeneratorControl.wait_acknowledged(pg):
                    raise TimeoutError("generator did not acknowledge the pause request")
            for attempt in range(1, attempts + 1):
                fence_lsn = None
                if mode == "exact":
                    _, fence_lsn = _write_fence(pg, settings)
                    _wait_slot_confirms(pg, settings, fence_lsn, timeout_s)
                token, lsn = _write_fence(pg, settings)
                fence_lsn = fence_lsn or lsn
                _wait_silver_sees(tconn, settings, token, timeout_s)
                cut = (
                    None
                    if mode == "exact"
                    else datetime.now(timezone.utc) - timedelta(minutes=settle_minutes)
                )
                pg.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                results = [compare_table(pg, tconn, settings, t, cut) for t in BUSINESS_TABLES]
                pg.commit()
                ok = all(r.ok for r in results)
                if ok or attempt == attempts:
                    break
                logger.warning("reconciliation mismatch; retrying", extra={"attempt": attempt})
                time.sleep(10)
        finally:
            if paused:
                GeneratorControl.request(pg, paused=False, by="reconcile")
    report = Report(
        mode,
        ok,
        started_at.isoformat(),
        cut.isoformat() if cut else None,
        fence_lsn,
        results,
        round(time.monotonic() - started, 1),
    )
    logger.info(
        "reconciliation finished",
        extra={
            "mode": mode,
            "ok": ok,
            "tables": len(results),
            "rows_source": sum(r.rows_source for r in results),
            "rows_lakehouse": sum(r.rows_lakehouse for r in results),
            "mismatched": [r.table for r in results if not r.ok],
        },
    )
    return report


def format_report(report: Report) -> str:
    lines = [
        f"reconciliation ({report.mode}) {'PASSED' if report.ok else 'FAILED'} in {report.seconds}s"
        + (f", fence LSN {report.fence_lsn}" if report.fence_lsn else "")
    ]
    lines.append(f"{'table':<22} {'source rows':>12} {'silver rows':>12}  result")
    for r in report.tables:
        lines.append(
            f"{r.table:<22} {r.rows_source:>12,} {r.rows_lakehouse:>12,}  {'ok' if r.ok else 'MISMATCH'}"
        )
        lines += [f"    - {m}" for m in r.mismatches] + [f"    ~ {w}" for w in r.warnings]
    return "\n".join(lines)
