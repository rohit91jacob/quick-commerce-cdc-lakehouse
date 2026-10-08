"""Migrations + generator against a real Postgres (CI: service container; locally: QC_PG_* env vars)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

psycopg = pytest.importorskip("psycopg")

from qcommerce.db import migrate  # noqa: E402
from qcommerce.generator import runner  # noqa: E402
from qcommerce.settings import GeneratorSettings, PostgresSettings  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture
def settings():
    base = PostgresSettings()
    try:
        admin = psycopg.connect(
            base.dsn(admin=True).replace(f"dbname={base.database}", "dbname=postgres"),
            autocommit=True,
            connect_timeout=5,
        )
    except psycopg.OperationalError as exc:
        pytest.skip(f"no Postgres available: {exc}")
    name = f"qc_it_{uuid.uuid4().hex[:8]}"
    admin.execute(f"CREATE DATABASE {name}")
    pg = PostgresSettings(database=name)
    gen = GeneratorSettings(
        seed=5,
        cities=1,
        stores_per_city=2,
        products=60,
        customers=150,
        riders_per_store=12,
        base_orders_per_store_hour=20.0,
    )
    yield SimpleNamespace(pg=pg, generator=gen)
    admin.execute(f"DROP DATABASE {name} WITH (FORCE)")
    admin.close()


def test_migrate_is_idempotent_and_stepwise(settings) -> None:
    assert migrate.migrate(settings.pg, target=1) == [1]
    assert migrate.migrate(settings.pg, target=1) == []
    assert migrate.migrate(settings.pg) == [2]
    assert [done for _, _, done in migrate.status(settings.pg)] == [True, True]


def test_seed_backfill_and_resume(settings) -> None:
    migrate.migrate(settings.pg, target=1)
    assert runner.seed(settings) is True
    assert runner.seed(settings) is False  # idempotent
    first = runner.run(settings, backfill_hours=3, live_minutes=0)
    assert first["orders_placed"] > 20
    # A second process resumes from the database: in-flight orders are closed out, ids continue.
    second = runner.run(settings, backfill_hours=0, live_minutes=0.05, schema_change_after_minutes=0)
    assert second["transactions"] >= 1
    with psycopg.connect(settings.pg.dsn()) as conn:
        open_orders = conn.execute(
            "SELECT count(*) FROM commerce.orders WHERE status NOT IN ('delivered', 'cancelled') "
            "AND placed_at < now() - interval '1 minute'"
        ).fetchone()[0]
        assert open_orders == 0
        bad_totals = conn.execute(
            "SELECT count(*) FROM commerce.orders WHERE total <> subtotal - discount + delivery_fee"
        ).fetchone()[0]
        assert bad_totals == 0
        has_tip = conn.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'tip_amount'"
        ).fetchone()[0]
        assert has_tip == 1
