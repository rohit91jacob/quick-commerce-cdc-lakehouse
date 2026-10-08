"""Content fingerprints of bronze/silver tables, used to prove that replays leave them unchanged."""

from __future__ import annotations

from qcommerce import trino_client
from qcommerce.settings import Settings

# Columns that legitimately differ between two writes of the same change.
VOLATILE = {"_ingested_at", "_batch_id", "_kafka_partition", "_kafka_offset"}


def table_checksum(conn, schema: str, table: str) -> tuple[int, str]:
    columns = [
        r[0]
        for r in trino_client.query(
            conn,
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema, table],
        )
        if r[0] not in VOLATILE
    ]
    parts = ", ".join(f"coalesce(cast(\"{c}\" AS varchar), '<null>')" for c in columns)
    count, digest = trino_client.query(
        conn, f"SELECT count(*), to_hex(checksum(to_utf8(concat_ws('|', {parts})))) FROM {schema}.\"{table}\""
    )[0]
    return int(count), digest or ""


def layer_checksums(settings: Settings, schema: str) -> dict[str, tuple[int, str]]:
    conn = trino_client.connect(settings.trino)
    tables = [
        r[0]
        for r in trino_client.query(
            conn,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name",
            [schema],
        )
    ]
    return {t: table_checksum(conn, schema, t) for t in tables if t != "cdc_heartbeat"}
