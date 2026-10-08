"""Iceberg table maintenance through Trino: compaction, snapshot expiry, orphan-file cleanup.

Streaming MERGEs leave many small data files and position-delete files behind. ``optimize`` rewrites
them (applying deletes), ``expire_snapshots`` bounds metadata growth and time-travel history, and
``remove_orphan_files`` deletes files no snapshot references (e.g. from failed commits). Optionally,
silver tombstones older than the replay horizon are purged.

Maintenance commits can conflict with the streaming writer's commits; both sides retry, and every
operation is safe to repeat.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from qcommerce import log, trino_client
from qcommerce.settings import Settings

logger = log.get(__name__)


@dataclass
class MaintenanceResult:
    table: str
    seconds: float
    actions: list[str] = field(default_factory=list)
    error: str | None = None


def list_tables(conn, schema: str) -> list[str]:
    rows = trino_client.query(
        conn,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' "
        "ORDER BY table_name",
        [schema],
    )
    return [r[0] for r in rows]


def _execute(conn, sql: str, retries: int = 3) -> None:
    for attempt in range(retries + 1):
        try:
            trino_client.query(conn, sql)
            return
        except Exception as exc:
            if attempt == retries:
                raise
            logger.warning(
                "maintenance statement failed; retrying",
                extra={"sql": sql, "attempt": attempt + 1, "error": str(exc)[:300]},
            )
            time.sleep(2 * (attempt + 1))


def run(
    settings: Settings,
    *,
    retention: str = "7d",
    file_size_threshold: str = "64MB",
    purge_tombstones_older_than_days: int | None = None,
    schemas: tuple[str, ...] | None = None,
) -> list[MaintenanceResult]:
    conn = trino_client.connect(settings.trino)
    schemas = schemas or (settings.lake.bronze_namespace, settings.lake.silver_namespace)
    results: list[MaintenanceResult] = []
    for schema in schemas:
        for table in list_tables(conn, schema):
            fqn = f'{schema}."{table}"'
            started = time.monotonic()
            result = MaintenanceResult(f"{schema}.{table}", 0.0)
            try:
                _execute(
                    conn,
                    f"ALTER TABLE {fqn} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')",
                )
                result.actions.append("optimize")
                if purge_tombstones_older_than_days is not None and schema == settings.lake.silver_namespace:
                    _execute(
                        conn,
                        f"DELETE FROM {fqn} WHERE _is_deleted AND _source_ts < current_timestamp - "
                        f"INTERVAL '{int(purge_tombstones_older_than_days)}' DAY",
                    )
                    result.actions.append("purge_tombstones")
                _execute(
                    conn, f"ALTER TABLE {fqn} EXECUTE expire_snapshots(retention_threshold => '{retention}')"
                )
                result.actions.append("expire_snapshots")
                _execute(
                    conn,
                    f"ALTER TABLE {fqn} EXECUTE remove_orphan_files(retention_threshold => '{retention}')",
                )
                result.actions.append("remove_orphan_files")
            except Exception as exc:
                result.error = str(exc)[:500]
                logger.error("maintenance failed", extra={"table": result.table, "error": result.error})
            result.seconds = round(time.monotonic() - started, 2)
            results.append(result)
    logger.info(
        "maintenance finished",
        extra={
            "tables": len(results),
            "failed": [r.table for r in results if r.error],
            "seconds": round(sum(r.seconds for r in results), 1),
        },
    )
    return results
