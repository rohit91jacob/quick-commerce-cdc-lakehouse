"""Facts about the OLTP schema shared by the generator tests, reconciliation and docs."""

from __future__ import annotations

PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "cities": ("city_id",),
    "dark_stores": ("store_id",),
    "categories": ("category_id",),
    "products": ("product_id",),
    "customers": ("customer_id",),
    "customer_addresses": ("address_id",),
    "inventory": ("store_id", "product_id"),
    "promotions": ("promo_id",),
    "riders": ("rider_id",),
    "rider_shifts": ("shift_id",),
    "orders": ("order_id",),
    "order_items": ("order_item_id",),
    "order_status_history": ("status_event_id",),
    "delivery_assignments": ("assignment_id",),
    "payments": ("payment_id",),
    "refunds": ("refund_id",),
    # V003: real public market data (Open Prices, Agmarknet).
    "market_locations": ("location_id",),
    "market_products": ("market_product_id",),
    "market_prices": ("price_id",),
    "mandi_prices": ("state", "district", "market", "commodity", "variety", "grade", "arrival_date"),
}

BUSINESS_TABLES: tuple[str, ...] = tuple(PRIMARY_KEYS)
