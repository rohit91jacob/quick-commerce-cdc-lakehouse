"""Parsing and mapping of the real public data sources (no network)."""

from __future__ import annotations

import json
from decimal import Decimal
from importlib import resources
from pathlib import Path

from qcommerce.realdata import agmarknet, openprices
from qcommerce.realdata.catalogue import load_snapshot, to_real_product
from qcommerce.realdata.categories import map_category

FIXTURES = Path(str(resources.files("qcommerce.realdata").joinpath("fixtures")))


def test_category_rules_prefer_specific_tags() -> None:
    assert map_category(["en:beverages", "en:teas"]) == "Tea, Coffee & Breakfast"
    assert map_category(["en:beverages", "en:sodas"]) == "Cold Drinks & Juices"
    assert map_category(["en:snacks", "en:chips-and-fries"]) == "Snacks & Munchies"
    assert map_category(["en:cosmetics"]) is None
    assert map_category(None) is None


def test_catalogue_items_must_be_indian_and_complete() -> None:
    raw = {"code": "8901063142428", "product_name": "Masala Noodles", "brands": "Maggi, Nestle",
           "quantity": "70 g", "categories_tags": ["en:instant-noodles"], "nutriscore_grade": "d"}  # fmt: skip
    item = to_real_product(raw)
    assert item is not None
    assert (item.brand, item.category, item.nutriscore_grade) == ("Maggi", "Instant & Frozen Food", "d")
    assert to_real_product({**raw, "code": "5449000000996"}) is None  # not a GS1 India barcode
    assert to_real_product({**raw, "code": "5449000000996"}, priced_in_india=True) is not None
    assert to_real_product({**raw, "brands": ""}) is None


def test_committed_snapshot_is_real_and_well_formed() -> None:
    products = load_snapshot()
    assert len(products) >= 150
    assert len({p.code for p in products}) == len(products)
    assert all(p.code.isdigit() and p.name and p.brand and p.category for p in products)
    assert any(p.has_inr_price for p in products)


def test_open_prices_fixture_parses_into_rows() -> None:
    batch = openprices.Batch()
    items = [item for page in openprices.fetch_fixtures(FIXTURES) for item in page]
    for item in items:
        openprices.parse_item(item, batch)
    assert len(batch.prices) + batch.skipped == len(items)
    assert batch.prices and batch.products and batch.locations
    assert batch.max_created == max(i["created"] for i in items if not i.get("duplicate_of"))
    assert all(p.price > 0 and len(p.currency) == 3 for p in batch.prices.values())
    assert any(p.currency == "INR" for p in batch.prices.values())
    for price in batch.prices.values():
        if price.market_product_id is not None:
            assert price.market_product_id in batch.products
        if price.location_id is not None:
            assert price.location_id in batch.locations


def test_open_prices_rejects_duplicates_and_bad_prices() -> None:
    page = json.loads((FIXTURES / "open_prices_001.json").read_text(encoding="utf-8"))["items"]
    good = dict(page[0])
    batch = openprices.Batch()
    for bad in ({**good, "duplicate_of": 1}, {**good, "price": None}, {**good, "currency": "RUPEES"}):
        openprices.parse_item(bad, batch)
    assert batch.skipped == 3 and not batch.prices


def test_agmarknet_record_parsing() -> None:
    record = {"state": "Karnataka", "district": "Bangalore", "market": "Binny Mill (F&V), Bangalore",
              "commodity": "Tomato", "variety": "Local", "grade": "FAQ", "arrival_date": "08/10/2026",
              "min_price": "1200", "max_price": "1800", "modal_price": "1500"}  # fmt: skip
    parsed = agmarknet.parse_record(record)
    assert parsed is not None
    assert (parsed.commodity, str(parsed.arrival_date), parsed.modal_price) == (
        "Tomato",
        "2026-10-08",
        Decimal("1500.00"),
    )
    assert agmarknet.parse_record({**record, "arrival_date": "yesterday"}) is None
    assert agmarknet.parse_record({**record, "market": ""}) is None
