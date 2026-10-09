"""Open Prices (https://prices.openfoodfacts.org): crowdsourced shelf prices with proof photos.

Fetched incrementally by ``created`` time (the API's ``created__gte`` filter, ascending), so each run
asks only for observations newer than the stored cursor. The boundary is re-read inclusively and
the upsert is idempotent, so nothing is lost or double-counted. Data licence: ODbL 1.0.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from qcommerce.realdata.categories import map_category
from qcommerce.realdata.http import PoliteClient

API_URL = "https://prices.openfoodfacts.org/api/v1/prices"
GRADES = {"a", "b", "c", "d", "e"}


@dataclass(frozen=True)
class MarketLocation:
    location_id: int
    osm_name: str | None
    brand: str | None
    city: str | None
    country_code: str | None
    lat: float | None
    lon: float | None


@dataclass(frozen=True)
class MarketProduct:
    market_product_id: int
    code: str
    name: str | None
    brand: str | None
    category_tag: str | None
    store_category: str | None
    quantity: Decimal | None
    quantity_unit: str | None
    nutriscore_grade: str | None


@dataclass(frozen=True)
class MarketPrice:
    price_id: int
    market_product_id: int | None
    location_id: int | None
    product_code: str | None
    category_tag: str | None
    price: Decimal
    currency: str
    is_discounted: bool
    price_without_discount: Decimal | None
    price_per: str | None
    observed_on: date
    source_created_at: datetime


@dataclass
class Batch:
    locations: dict[int, MarketLocation] = field(default_factory=dict)
    products: dict[int, MarketProduct] = field(default_factory=dict)
    prices: dict[int, MarketPrice] = field(default_factory=dict)
    max_created: str | None = None
    skipped: int = 0


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _money(value: Any) -> Decimal | None:
    if value is None:
        return None
    amount = Decimal(str(value)).quantize(Decimal("0.01"))
    return amount if Decimal("0") < amount < Decimal("100000000000") else None


def _text(value: Any, limit: int = 200) -> str | None:
    cleaned = " ".join(str(value or "").split())
    return cleaned[:limit] or None


def _first_en_tag(tags: list[str] | None) -> str | None:
    for tag in tags or []:
        if tag.startswith("en:"):
            return tag[:80]
    return None


def parse_item(item: dict[str, Any], batch: Batch) -> None:
    """Add one API price item (with its embedded product and location) to ``batch``."""
    price = _money(item.get("price"))
    currency = str(item.get("currency") or "").upper()
    if item.get("duplicate_of") or price is None or len(currency) != 3 or not item.get("date"):
        batch.skipped += 1
        return
    location = item.get("location") or {}
    location_id = int(item["location_id"]) if item.get("location_id") and location else None
    if location_id is not None:
        batch.locations[location_id] = MarketLocation(
            location_id=location_id,
            osm_name=_text(location.get("osm_name")),
            brand=_text(location.get("osm_brand"), 80),
            city=_text(location.get("osm_address_city"), 80),
            country_code=(_text(location.get("osm_address_country_code"), 2) or "").upper() or None,
            lat=location.get("osm_lat"),
            lon=location.get("osm_lon"),
        )
    product = item.get("product") or {}
    product_id = int(item["product_id"]) if item.get("product_id") and product else None
    if product_id is not None:
        tags = product.get("categories_tags")
        quantity = product.get("product_quantity")
        grade = str(product.get("nutriscore_grade") or "")
        batch.products[product_id] = MarketProduct(
            market_product_id=product_id,
            code=str(product.get("code") or item.get("product_code") or ""),
            name=_text(product.get("product_name") or item.get("product_name")),
            brand=_text(str(product.get("brands") or "").split(",")[0], 80),
            category_tag=_first_en_tag(tags),
            store_category=map_category(tags),
            quantity=Decimal(str(quantity)).quantize(Decimal("0.001")) if quantity else None,
            quantity_unit=_text(product.get("product_quantity_unit"), 10),
            nutriscore_grade=grade if grade in GRADES else None,
        )
    batch.prices[int(item["id"])] = MarketPrice(
        price_id=int(item["id"]),
        market_product_id=product_id,
        location_id=location_id,
        product_code=_text(item.get("product_code"), 40),
        category_tag=_text(item.get("category_tag"), 80),
        price=price,
        currency=currency,
        is_discounted=bool(item.get("price_is_discounted")),
        price_without_discount=_money(item.get("price_without_discount")),
        price_per=_text(item.get("price_per"), 20),
        observed_on=date.fromisoformat(str(item["date"])[:10]),
        source_created_at=_ts(item["created"]),
    )
    if batch.max_created is None or item["created"] > batch.max_created:
        batch.max_created = item["created"]


def fetch(
    since: str,
    *,
    client: PoliteClient | None = None,
    page_size: int = 100,
    max_pages: int = 150,
    currency: str | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Pages of price items created at or after ``since`` (ISO-8601), oldest first."""
    client = client or PoliteClient(min_interval_s=0.5)
    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "created__gte": since,
            "order_by": "created",
            "size": page_size,
            "page": page,
        }
        if currency:
            params["currency"] = currency
        data = client.get_json(API_URL, params)
        items = data.get("items") or []
        if items:
            yield items
        if not items or page >= int(data.get("pages") or 0):
            return


def fetch_fixtures(directory: Path) -> Iterator[list[dict[str, Any]]]:
    """Offline mode for tests and CI: pages saved as ``open_prices_*.json``."""
    for path in sorted(directory.glob("open_prices_*.json")):
        yield json.loads(path.read_text(encoding="utf-8"))["items"]
