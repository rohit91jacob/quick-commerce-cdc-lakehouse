"""``qc realdata sync``: apply new real observations to the OLTP, so CDC carries real changes.

One run:
  1. applies pending migrations (V003 adds the market tables);
  2. links real catalogue products (seeded as ``OFF-<barcode>`` SKUs) to their Open Food Facts data;
  3. pulls Open Prices observations created since the stored cursor and upserts them, page by page,
     each page and its cursor advance in one transaction (a crash re-reads at most one page);
  4. moves the selling price of every real catalogue product to its latest real INR shelf price;
  5. optionally upserts today's Agmarknet mandi prices.

Upserts only touch rows whose values changed, so a re-run produces no CDC events.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg

from qcommerce import log
from qcommerce.db import migrate as migrations
from qcommerce.realdata import agmarknet, openprices
from qcommerce.realdata.catalogue import load_snapshot
from qcommerce.settings import Settings

logger = log.get(__name__)

CURSOR = "open_prices"
# Indian (INR) observations are rare, so they get their own stream over the whole history: that is
# what prices the store's real catalogue. The global stream only looks back ``backfill_days``.
INR_CURSOR = "open_prices_inr"
INR_EPOCH = "2020-01-01T00:00:00Z"
MANDI_KEY_ENV = "QC_DATA_GOV_IN_API_KEY"


def _upsert(conn: psycopg.Connection, table: str, key: tuple[str, ...], row: dict[str, Any]) -> str:
    """Insert or update one row; returns 'inserted', 'updated' or 'unchanged'."""
    cols = list(row)
    values = [row[c] for c in cols]
    data_cols = [c for c in cols if c not in key]
    sql = (
        f"INSERT INTO commerce.{table} ({', '.join(cols)}, created_at, updated_at) "
        f"VALUES ({', '.join(['%s'] * len(cols))}, now(), now()) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in data_cols)
        + ", updated_at = now() "
        f"WHERE ({', '.join(f'{table}.{c}' for c in data_cols)}) "
        f"IS DISTINCT FROM ({', '.join(f'EXCLUDED.{c}' for c in data_cols)}) "
        "RETURNING (xmax = 0)"
    )
    returned = conn.execute(sql, values).fetchone()
    if returned is None:
        return "unchanged"
    return "inserted" if returned[0] else "updated"


def get_cursor(conn: psycopg.Connection, source: str) -> str | None:
    row = conn.execute("SELECT cursor FROM ops.ingest_cursors WHERE source = %s", (source,)).fetchone()
    return row[0] if row else None


def set_cursor(conn: psycopg.Connection, source: str, value: str) -> None:
    conn.execute(
        "INSERT INTO ops.ingest_cursors (source, cursor, updated_at) VALUES (%s, %s, now()) "
        "ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()",
        (source, value),
    )


def link_catalogue(conn: psycopg.Connection) -> int:
    """Stamp real catalogue products with their barcode, Nutri-Score and image (idempotent)."""
    linked = 0
    by_code = {p.code: p for p in load_snapshot()}
    rows = conn.execute(
        "SELECT product_id, sku FROM commerce.products WHERE sku LIKE 'OFF-%' AND off_code IS NULL"
    ).fetchall()
    for product_id, sku in rows:
        code = sku.removeprefix("OFF-")
        real = by_code.get(code)
        conn.execute(
            "UPDATE commerce.products SET off_code = %s, catalogue_source = 'open_food_facts', "
            "nutriscore_grade = %s, image_url = %s, updated_at = now() WHERE product_id = %s",
            (code, real.nutriscore_grade if real else None, real.image_url if real else None, product_id),
        )
        linked += 1
    return linked


CATALOGUE_PRICES = """
WITH latest AS (
    SELECT DISTINCT ON (product_code) product_code, price, price_without_discount
    FROM commerce.market_prices
    WHERE currency = 'INR' AND product_code IS NOT NULL
    ORDER BY product_code, observed_on DESC, source_created_at DESC, price_id DESC
)
UPDATE commerce.products p
SET selling_price = l.price,
    mrp = greatest(p.mrp, l.price, coalesce(l.price_without_discount, l.price)),
    price_source = 'open_prices',
    updated_at = now()
FROM latest l
WHERE p.off_code = l.product_code
  AND (p.selling_price, p.price_source) IS DISTINCT FROM (l.price, 'open_prices')
RETURNING p.product_id
"""


def apply_batch(conn: psycopg.Connection, batch: openprices.Batch) -> dict[str, int]:
    counts: dict[str, int] = {}

    def bump(kind: str, outcome: str) -> None:
        counts[f"{kind}_{outcome}"] = counts.get(f"{kind}_{outcome}", 0) + 1

    for loc in batch.locations.values():
        bump("locations", _upsert(conn, "market_locations", ("location_id",), loc.__dict__))
    for product in batch.products.values():
        bump("products", _upsert(conn, "market_products", ("market_product_id",), product.__dict__))
    for price in batch.prices.values():
        bump("prices", _upsert(conn, "market_prices", ("price_id",), price.__dict__))
    return counts


def sync(
    settings: Settings,
    *,
    fixtures: Path | None = None,
    backfill_days: float = 14.0,
    max_pages: int = 150,
    now: datetime | None = None,
) -> dict[str, Any]:
    migrations.migrate(settings.pg)
    started = now or datetime.now(timezone.utc)
    summary: dict[str, Any] = {"source_mode": "fixtures" if fixtures else "live"}
    with psycopg.connect(settings.pg.dsn()) as conn:
        with conn.transaction():
            summary["catalogue_linked"] = link_catalogue(conn)
        cursor_before = get_cursor(conn, CURSOR)
        since = cursor_before or (started - timedelta(days=backfill_days)).isoformat(timespec="seconds")
        summary["cursor_before"], summary["fetched_since"] = cursor_before, since
        if fixtures:
            streams = [(CURSOR, cursor_before, openprices.fetch_fixtures(fixtures))]
        else:
            inr_before = get_cursor(conn, INR_CURSOR)
            summary["inr_cursor_before"] = inr_before
            streams = [
                (CURSOR, cursor_before, openprices.fetch(since, max_pages=max_pages)),
                (
                    INR_CURSOR,
                    inr_before,
                    openprices.fetch(inr_before or INR_EPOCH, max_pages=max_pages, currency="INR"),
                ),
            ]
        totals: dict[str, int] = {"pages": 0, "items": 0, "skipped": 0}
        for name, start_cursor, pages in streams:
            cursor = start_cursor
            for items in pages:
                batch = openprices.Batch()
                for item in items:
                    openprices.parse_item(item, batch)
                with conn.transaction():
                    for key, value in apply_batch(conn, batch).items():
                        totals[key] = totals.get(key, 0) + value
                    if batch.max_created and (cursor is None or batch.max_created > cursor):
                        cursor = batch.max_created
                        set_cursor(conn, name, cursor)
                totals["pages"] += 1
                totals["items"] += len(items)
                totals["skipped"] += batch.skipped
            summary["cursor_after" if name == CURSOR else "inr_cursor_after"] = cursor
        summary.update(totals)
        with conn.transaction():
            summary["catalogue_price_updates"] = len(conn.execute(CATALOGUE_PRICES).fetchall())
        key = os.environ.get(MANDI_KEY_ENV, "").strip()
        if key:
            mandi: dict[str, int] = {}
            with conn.transaction():
                for record in agmarknet.fetch(key):
                    outcome = _upsert(
                        conn,
                        "mandi_prices",
                        ("state", "district", "market", "commodity", "variety", "grade", "arrival_date"),
                        record.__dict__,
                    )
                    mandi[outcome] = mandi.get(outcome, 0) + 1
            summary["mandi"] = mandi
        else:
            summary["mandi"] = f"skipped ({MANDI_KEY_ENV} not set)"
        summary["real_catalogue_products"], summary["real_priced_products"] = conn.execute(
            "SELECT count(*) FILTER (WHERE catalogue_source = 'open_food_facts'), "
            "count(*) FILTER (WHERE price_source = 'open_prices') FROM commerce.products"
        ).fetchone()
    logger.info("realdata sync finished", extra=summary)
    return summary
