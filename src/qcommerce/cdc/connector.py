"""Debezium Postgres connector: configuration, registration and health.

Serialization choice (see docs/adr/0002-json-with-embedded-schemas.md): the JSON converter with
``schemas.enable=true``. Every message carries its Kafka Connect schema, so the writer can derive exact
column types (and detect new columns) without a schema registry; the size overhead is mostly removed by
producer-side zstd compression.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import psycopg
import requests

from qcommerce import log
from qcommerce.settings import Settings

logger = log.get(__name__)

# Tables whose topics get several partitions (per-key ordering is preserved by key-based partitioning).
HOT_TABLES = ("orders", "order_items", "order_status_history", "inventory", "payments", "riders")


def connector_config(settings: Settings) -> dict[str, Any]:
    pg, kafka, connect = settings.pg, settings.kafka, settings.connect
    prefix = kafka.topic_prefix
    hot = "|".join(HOT_TABLES)
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": connect.db_host or pg.host,
        "database.port": str(connect.db_port or pg.port),
        "database.user": pg.debezium_user,
        "database.password": pg.debezium_password.get_secret_value(),
        "database.dbname": pg.database,
        "topic.prefix": prefix,
        "plugin.name": "pgoutput",
        "publication.name": pg.publication,
        "publication.autocreate.mode": "disabled",  # created by migration V001 with an explicit table list
        "slot.name": pg.slot,
        "schema.include.list": pg.app_schema,
        "snapshot.mode": "initial",
        # Value encodings the writer decodes (qcommerce.lakehouse.connect_schema).
        "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive_time_microseconds",
        # Precision/scale of NUMERIC columns (as schema parameters), so decimals keep their exact type.
        "datatype.propagate.source.type": r"(?i)(.+\.)?numeric",
        "tombstones.on.delete": "true",
        # Keeps the slot advancing (and WAL from piling up) while business tables are quiet.
        "heartbeat.interval.ms": "5000",
        "heartbeat.action.query": f"UPDATE {pg.app_schema}.cdc_heartbeat SET beat_at = now() WHERE id = 1",
        "signal.data.collection": f"{pg.app_schema}.debezium_signal",
        "incremental.snapshot.chunk.size": "2048",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
        "producer.override.compression.type": "zstd",
        "producer.override.linger.ms": "20",
        "topic.creation.enable": "true",
        "topic.creation.default.replication.factor": "-1",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.cleanup.policy": "delete",
        "topic.creation.default.retention.ms": str(7 * 24 * 3600 * 1000),
        "topic.creation.groups": "hot",
        "topic.creation.hot.include": rf"{prefix}\.{pg.app_schema}\.({hot})",
        "topic.creation.hot.partitions": str(connect.topic_partitions_hot),
        "topic.creation.hot.replication.factor": "-1",
        "errors.log.enable": "true",
        "errors.log.include.messages": "false",
    }


class ConnectClient:
    def __init__(self, url: str, timeout: float = 15.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        return requests.request(method, f"{self.url}{path}", timeout=self.timeout, **kwargs)

    def wait_ready(self, timeout_s: float = 180.0) -> None:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                if self._request("GET", "/connector-plugins").ok:
                    return
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"Kafka Connect at {self.url} not ready after {timeout_s}s")
            time.sleep(2)

    def upsert(self, name: str, config: dict[str, Any]) -> None:
        response = self._request("PUT", f"/connectors/{name}/config", json=config)
        if response.status_code >= 400:
            raise RuntimeError(f"connector config rejected ({response.status_code}): {response.text}")

    def delete(self, name: str) -> None:
        response = self._request("DELETE", f"/connectors/{name}")
        if response.status_code not in (204, 404):
            raise RuntimeError(f"connector delete failed ({response.status_code}): {response.text}")

    def status(self, name: str) -> dict[str, Any] | None:
        response = self._request("GET", f"/connectors/{name}/status")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def restart(self, name: str) -> None:
        self._request(
            "POST", f"/connectors/{name}/restart", params={"includeTasks": "true", "onlyFailed": "true"}
        )


def is_running(status: dict[str, Any] | None) -> bool:
    if not status:
        return False
    tasks = status.get("tasks", [])
    return (
        status["connector"]["state"] == "RUNNING"
        and bool(tasks)
        and all(t["state"] == "RUNNING" for t in tasks)
    )


def register(settings: Settings, *, wait_s: float = 180.0) -> dict[str, Any]:
    client = ConnectClient(settings.connect.url)
    client.wait_ready()
    client.upsert(settings.connect.connector_name, connector_config(settings))
    deadline = time.monotonic() + wait_s
    while True:
        status = client.status(settings.connect.connector_name)
        if is_running(status):
            logger.info("connector running", extra={"connector": settings.connect.connector_name})
            return status or {}
        failed = status and any(t["state"] == "FAILED" for t in status.get("tasks", []))
        if failed or time.monotonic() > deadline:
            raise RuntimeError(f"connector did not reach RUNNING: {status}")
        time.sleep(2)


def request_incremental_snapshot(settings: Settings, tables: list[str]) -> str:
    """Ask Debezium to re-read ``tables`` (e.g. ``commerce.orders``) without stopping streaming."""
    signal_id = f"adhoc-{uuid.uuid4()}"
    data = '{"data-collections": [' + ", ".join(f'"{t}"' for t in tables) + '], "type": "incremental"}'
    with psycopg.connect(settings.pg.dsn(), autocommit=True) as conn:
        conn.execute(
            f"INSERT INTO {settings.pg.app_schema}.debezium_signal (id, type, data) VALUES (%s, %s, %s)",
            (signal_id, "execute-snapshot", data),
        )
    return signal_id


def slot_lag(settings: Settings) -> dict[str, Any] | None:
    """Replication-slot health: retained WAL and how far Debezium's confirmed position trails."""
    with psycopg.connect(settings.pg.dsn(), autocommit=True) as conn:
        row = conn.execute(
            "SELECT active, wal_status, "
            "       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
            "       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (settings.pg.slot,),
        ).fetchone()
    if row is None:
        return None
    return {
        "active": row[0],
        "wal_status": row[1],
        "retained_wal_bytes": row[2],
        "confirmed_lag_bytes": row[3],
    }
