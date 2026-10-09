"""Optional: daily wholesale (mandi) prices from data.gov.in's Agmarknet feed.

Disabled unless ``QC_DATA_GOV_IN_API_KEY`` is set (free key: https://data.gov.in, sign up, then
My Account, API key). Prices are rupees per quintal. Licence: Government Open Data License - India.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from qcommerce.realdata.http import PoliteClient

# "Current daily price of various commodities from various markets (Mandi)"
RESOURCE_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"


@dataclass(frozen=True)
class MandiPrice:
    state: str
    district: str
    market: str
    commodity: str
    variety: str
    grade: str
    arrival_date: date
    min_price: Decimal
    max_price: Decimal
    modal_price: Decimal


def _text(value: Any) -> str:
    return " ".join(str(value or "").split())[:80]


def parse_record(record: dict[str, Any]) -> MandiPrice | None:
    try:
        arrival = datetime.strptime(_text(record.get("arrival_date")), "%d/%m/%Y").date()
        low, high, modal = (
            Decimal(str(record[k])).quantize(Decimal("0.01"))
            for k in ("min_price", "max_price", "modal_price")
        )
    except (KeyError, ValueError, InvalidOperation):
        return None
    state, district, market, commodity, variety, grade = (
        _text(record.get(k)) for k in ("state", "district", "market", "commodity", "variety", "grade")
    )
    if not market or not commodity or min(low, high, modal) < 0:
        return None
    return MandiPrice(state, district, market, commodity, variety, grade, arrival, low, high, modal)


def fetch(api_key: str, *, page_size: int = 500, max_pages: int = 40) -> Iterator[MandiPrice]:
    client = PoliteClient(min_interval_s=1.0)
    for page in range(max_pages):
        data = client.get_json(
            RESOURCE_URL,
            {"api-key": api_key, "format": "json", "limit": page_size, "offset": page * page_size},
        )
        records = data.get("records") or []
        for record in records:
            parsed = parse_record(record)
            if parsed:
                yield parsed
        if len(records) < page_size:
            return
