"""The store catalogue's real products: a committed snapshot of Open Food Facts products sold in India.

The snapshot is refreshed with ``qc realdata catalogue-snapshot`` (a few minutes, rate-limited);
seeding reads the committed file, so a seed is deterministic and needs no network.
Open Food Facts data: Open Database License 1.0 (database) and Database Contents License (contents).
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any

from qcommerce import log
from qcommerce.realdata.categories import map_category
from qcommerce.realdata.http import FetchError, PoliteClient

logger = log.get(__name__)

SNAPSHOT = "off_india_catalogue.json.gz"
SEARCH_URL = "https://world.openfoodfacts.org/api/v2/search"
PRICES_URL = "https://prices.openfoodfacts.org/api/v1/prices"
FIELDS = (
    "code,product_name,product_name_en,brands,quantity,categories_tags,nutriscore_grade,image_front_small_url"
)
INDIA_GS1_PREFIX = "890"  # barcodes issued by GS1 India


@dataclass(frozen=True)
class RealProduct:
    code: str
    name: str
    brand: str
    quantity: str
    category: str
    nutriscore_grade: str | None
    image_url: str | None
    has_inr_price: bool = False


def _clean(text: Any) -> str:
    return " ".join(str(text or "").split())


def to_real_product(raw: dict[str, Any], *, priced_in_india: bool = False) -> RealProduct | None:
    """An Open Food Facts product -> catalogue item, or None if it is not a usable Indian product.

    Products already observed at an Indian shop (``priced_in_india``) qualify whatever their barcode.
    """
    code = _clean(raw.get("code"))
    name = _clean(raw.get("product_name_en") or raw.get("product_name"))
    brand = _clean(str(raw.get("brands") or "").split(",")[0])
    category = map_category(raw.get("categories_tags"))
    indian = priced_in_india or code.startswith(INDIA_GS1_PREFIX)
    if not (indian and code.isdigit() and name and brand and category):
        return None
    quantity = _clean(raw.get("quantity"))
    if not quantity and raw.get("product_quantity"):
        quantity = f"{raw['product_quantity']:g} {raw.get('product_quantity_unit') or ''}".strip()
    grade = _clean(raw.get("nutriscore_grade")).lower() or None
    return RealProduct(
        code=code,
        name=name[:120],
        brand=brand[:60],
        quantity=quantity[:30] or "1 pack",
        category=category,
        nutriscore_grade=grade if grade in {"a", "b", "c", "d", "e"} else None,
        image_url=_clean(raw.get("image_front_small_url") or raw.get("image_url")) or None,
        has_inr_price=priced_in_india,
    )


def build_snapshot(
    out: Path, *, pages: int = 20, page_size: int = 100, per_category: int = 60
) -> dict[str, int]:
    # Open Food Facts asks for <= 10 search requests/minute; its search tier also answers 401/503
    # under load, so those are retried too.
    client = PoliteClient(min_interval_s=6.5, retries=3, extra_retry_status={401, 403})
    chosen: dict[str, RealProduct] = {}
    per_cat: dict[str, int] = {}
    seen = 0

    def keep(item: RealProduct | None) -> None:
        if item and item.code not in chosen and per_cat.get(item.category, 0) < per_category:
            chosen[item.code] = item
            per_cat[item.category] = per_cat.get(item.category, 0) + 1

    # First, every product that already has a real shelf price from an Indian shop (Open Prices
    # observations embed the Open Food Facts product), so those SKUs start with real prices.
    prices = PoliteClient(min_interval_s=0.5)
    for page in range(1, 50):
        data = prices.get_json(PRICES_URL, {"currency": "INR", "size": 100, "page": page})
        for price in data.get("items") or []:
            if price.get("product"):
                keep(to_real_product(price["product"], priced_in_india=True))
        if page >= int(data.get("pages") or 0):
            break
    priced = len(chosen)
    for page in range(1, pages + 1):
        try:
            data = client.get_json(
                SEARCH_URL,
                {"countries_tags_en": "india", "fields": FIELDS, "page_size": page_size, "page": page,
                 "sort_by": "unique_scans_n"},
            )  # fmt: skip
        except FetchError as exc:  # the search endpoint is intermittently overloaded; skip the page
            logger.warning("catalogue page skipped", extra={"page": page, "error": str(exc)})
            continue
        products = data.get("products") or []
        seen += len(products)
        for raw in products:
            keep(to_real_product(raw))
        if not products:
            break
    payload = {
        "source": "Open Food Facts (https://world.openfoodfacts.org), products sold in India with GS1 India barcodes",
        "license": "Open Database License 1.0 (database), Database Contents License 1.0 (contents)",
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        # Priced-in-India products first within each category, so seeding picks them first.
        "products": [
            item.__dict__
            for item in sorted(chosen.values(), key=lambda p: (p.category, not p.has_inr_price, p.code))
        ],
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=0)
    summary = {
        "scanned": seen,
        "priced_in_india": priced,
        "kept": len(chosen),
        **{f"category:{k}": v for k, v in sorted(per_cat.items())},
    }
    logger.info("catalogue snapshot written", extra=summary)
    return summary


def load_snapshot(path: Path | None = None) -> list[RealProduct]:
    """The committed snapshot (or ``path``). Empty if no snapshot is available."""
    try:
        if path is None:
            raw = resources.files("qcommerce.realdata").joinpath(SNAPSHOT).read_bytes()
        else:
            raw = path.read_bytes()
    except FileNotFoundError:
        return []
    payload = json.loads(gzip.decompress(raw).decode("utf-8"))
    return [RealProduct(**item) for item in payload["products"]]
