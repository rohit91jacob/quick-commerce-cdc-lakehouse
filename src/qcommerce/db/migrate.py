"""Versioned SQL migrations for the OLTP database, plus role/grant bootstrap.

Migrations live next to this module as ``V<NNN>__<name>.sql`` and are applied in order, each
in its own transaction, and recorded in ``ops.schema_migrations``. Applying is idempotent.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from importlib import resources

import psycopg
from psycopg import sql

from qcommerce import log
from qcommerce.settings import PostgresSettings

logger = log.get(__name__)

_NAME = re.compile(r"^V(\d{3})__([a-z0-9_]+)\.sql$")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


def available() -> list[Migration]:
    found = []
    for entry in resources.files("qcommerce.db.migrations").iterdir():
        match = _NAME.match(entry.name)
        if match:
            found.append(Migration(int(match.group(1)), match.group(2), entry.read_text(encoding="utf-8")))
    return sorted(found, key=lambda m: m.version)


def _ensure_ledger(conn: psycopg.Connection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS ops.schema_migrations (
               version     integer PRIMARY KEY,
               name        text        NOT NULL,
               applied_at  timestamptz NOT NULL DEFAULT now())"""
    )


def applied_versions(conn: psycopg.Connection) -> set[int]:
    _ensure_ledger(conn)
    return {row[0] for row in conn.execute("SELECT version FROM ops.schema_migrations")}


def _ensure_role(conn: psycopg.Connection, role: str, password: str, *, replication: bool) -> None:
    exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
    verb = "ALTER" if exists else "CREATE"
    options = sql.SQL("LOGIN REPLICATION") if replication else sql.SQL("LOGIN")
    conn.execute(
        sql.SQL("{} ROLE {} WITH {} PASSWORD {}").format(
            sql.SQL(verb), sql.Identifier(role), options, sql.Literal(password)
        )
    )


def _grant(conn: psycopg.Connection, settings: PostgresSettings) -> None:
    app, dbz = sql.Identifier(settings.app_user), sql.Identifier(settings.debezium_user)
    statements = [
        "GRANT USAGE ON SCHEMA commerce, ops TO {app}",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA commerce TO {app}",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ops TO {app}",
        "GRANT USAGE ON SCHEMA commerce TO {dbz}",
        "GRANT SELECT ON ALL TABLES IN SCHEMA commerce TO {dbz}",
        # heartbeat.action.query and incremental-snapshot watermarks write these two tables.
        "GRANT UPDATE ON commerce.cdc_heartbeat TO {dbz}",
        "GRANT INSERT, UPDATE, DELETE ON commerce.debezium_signal TO {dbz}",
    ]
    for statement in statements:
        conn.execute(sql.SQL(statement).format(app=app, dbz=dbz))


def migrate(settings: PostgresSettings, target: int | None = None) -> list[int]:
    """Apply pending migrations up to ``target`` (inclusive; default: all). Returns applied versions."""
    newly_applied: list[int] = []
    with psycopg.connect(settings.dsn(admin=True), autocommit=True) as conn:
        _ensure_role(conn, settings.app_user, settings.app_password.get_secret_value(), replication=False)
        _ensure_role(
            conn, settings.debezium_user, settings.debezium_password.get_secret_value(), replication=True
        )
        done = applied_versions(conn)
        for migration in available():
            if migration.version in done or (target is not None and migration.version > target):
                continue
            with conn.transaction():
                conn.execute(migration.sql)
                conn.execute(
                    "INSERT INTO ops.schema_migrations (version, name) VALUES (%s, %s)",
                    (migration.version, migration.name),
                )
            logger.info(
                "applied migration", extra={"version": migration.version, "migration": migration.name}
            )
            newly_applied.append(migration.version)
        if applied_versions(conn):
            _grant(conn, settings)
    return newly_applied


def status(settings: PostgresSettings) -> list[tuple[int, str, bool]]:
    with psycopg.connect(settings.dsn(admin=True), autocommit=True) as conn:
        done = applied_versions(conn)
    return [(m.version, m.name, m.version in done) for m in available()]
