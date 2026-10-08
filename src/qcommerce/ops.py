"""Operational health of the pipeline: connector, replication slot, writer lag, freshness, dead letters."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

from qcommerce import log, trino_client
from qcommerce.cdc import connector
from qcommerce.oltp import BUSINESS_TABLES
from qcommerce.settings import Settings

logger = log.get(__name__)


@dataclass
class Health:
    ok: bool
    checked_at: str
    connector: dict[str, Any] | None
    slot: dict[str, Any] | None
    writer: dict[str, Any] | None
    freshness_seconds: dict[str, float | None] = field(default_factory=dict)
    dead_letters: int | None = None
    problems: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def writer_progress(url: str) -> dict[str, Any] | None:
    try:
        response = requests.get(f"{url.rstrip('/')}/progress", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def silver_freshness(settings: Settings) -> dict[str, float | None]:
    """Seconds since each silver table last received a change (None if it does not exist yet)."""
    conn = trino_client.connect(settings.trino, schema=settings.lake.silver_namespace)
    now = datetime.now(timezone.utc)
    out: dict[str, float | None] = {}
    for table in BUSINESS_TABLES:
        try:
            value = trino_client.query(conn, f'SELECT max(_ingested_at) FROM "{table}"')[0][0]
        except Exception:
            value = None
        out[table] = round((now - value).total_seconds(), 1) if value else None
    return out


def dead_letter_count(settings: Settings) -> int | None:
    conn = trino_client.connect(settings.trino, schema=settings.lake.bronze_namespace)
    if not trino_client.table_exists(conn, settings.lake.bronze_namespace, "_dead_letters"):
        return 0
    return int(trino_client.query(conn, 'SELECT count(*) FROM "_dead_letters"')[0][0])


def check(
    settings: Settings,
    *,
    writer_url: str | None = None,
    max_slot_lag_bytes: int = 256 * 1024**2,
    max_offsets_behind: int = 50_000,
    max_staleness_s: float | None = None,
) -> Health:
    problems: list[str] = []
    try:
        status = connector.ConnectClient(settings.connect.url).status(settings.connect.connector_name)
        if not connector.is_running(status):
            problems.append(f"connector not RUNNING: {status}")
    except requests.RequestException as exc:
        status = None
        problems.append(f"Kafka Connect unreachable: {exc}")
    slot = connector.slot_lag(settings)
    if slot is None:
        problems.append(f"replication slot {settings.pg.slot} does not exist")
    else:
        if slot["wal_status"] == "lost":
            problems.append("replication slot lost its WAL (re-snapshot required, see runbook)")
        if (slot["confirmed_lag_bytes"] or 0) > max_slot_lag_bytes:
            problems.append(f"slot lag {slot['confirmed_lag_bytes']} bytes > {max_slot_lag_bytes}")
    writer = writer_progress(writer_url) if writer_url else None
    if writer_url and writer is None:
        problems.append(f"writer metrics endpoint {writer_url} unreachable")
    if writer:
        for source in (writer.get("progress") or {}).get("sources", []):
            behind = float((source.get("metrics") or {}).get("maxOffsetsBehindLatest", 0))
            if behind > max_offsets_behind:
                problems.append(f"writer {behind:.0f} offsets behind latest")
    freshness = silver_freshness(settings)
    if max_staleness_s is not None:
        stale = {
            t: s
            for t, s in freshness.items()
            if s is not None and s > max_staleness_s and t in ("orders", "order_status_history", "inventory")
        }
        if stale:
            problems.append(f"stale silver tables: {stale}")
    dead = dead_letter_count(settings)
    if dead:
        problems.append(f"{dead} dead-lettered records")
    return Health(
        not problems, datetime.now(timezone.utc).isoformat(), status, slot, writer, freshness, dead, problems
    )
