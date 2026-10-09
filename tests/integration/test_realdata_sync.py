"""`qc realdata sync` against a real Postgres, using the committed (real) Open Prices fixtures."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from qcommerce.db import migrate  # noqa: E402
from qcommerce.generator import runner  # noqa: E402
from qcommerce.realdata import sync  # noqa: E402
from tests.integration.test_generator_postgres import settings  # noqa: E402,F401  (fixture)

pytestmark = pytest.mark.integration
FIXTURES = Path(str(resources.files("qcommerce.realdata").joinpath("fixtures")))


def test_sync_is_incremental_idempotent_and_prices_the_catalogue(settings) -> None:  # noqa: F811
    migrate.migrate(settings.pg, target=1)
    runner.seed(settings)
    first = sync.sync(settings, fixtures=FIXTURES)
    assert first["real_catalogue_products"] > 0
    assert first["prices_inserted"] == 80 - first["skipped"]
    assert first["cursor_after"] and first["cursor_before"] is None
    # Re-applying the same observations changes nothing (no CDC noise) and keeps the cursor.
    second = sync.sync(settings, fixtures=FIXTURES)
    assert second.get("prices_inserted", 0) == 0 and second.get("prices_updated", 0) == 0
    assert second["prices_unchanged"] == first["prices_inserted"]
    assert second["catalogue_price_updates"] == 0
    assert second["cursor_after"] == first["cursor_after"]
    with psycopg.connect(settings.pg.dsn()) as conn:
        mismatched = conn.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (product_code) product_code, price FROM commerce.market_prices
                WHERE currency = 'INR' ORDER BY product_code, observed_on DESC, source_created_at DESC, price_id DESC)
            SELECT count(*) FROM commerce.products p JOIN latest l ON l.product_code = p.off_code
            WHERE p.selling_price <> l.price OR p.price_source <> 'open_prices'
            """
        ).fetchone()[0]
        assert mismatched == 0
        priced = conn.execute(
            "SELECT count(*) FROM commerce.products WHERE price_source = 'open_prices'"
        ).fetchone()[0]
        assert priced == first["real_priced_products"]
        assert (
            conn.execute("SELECT count(*) FROM commerce.products WHERE selling_price > mrp").fetchone()[0]
            == 0
        )
    # Orders placed afterwards are priced from the (partly real) catalogue.
    run = runner.run(settings, backfill_hours=1, live_minutes=0)
    assert run["orders_placed"] > 0
