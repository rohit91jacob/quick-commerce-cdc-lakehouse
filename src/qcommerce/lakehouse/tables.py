"""Per-table layout hints. Everything else (columns, keys, types) comes from the CDC stream itself."""

from __future__ import annotations

# Parents before children, so a micro-batch never exposes an order item whose order is not in silver yet.
LOAD_ORDER: tuple[str, ...] = (
    "cities",
    "categories",
    "dark_stores",
    "products",
    "customers",
    "customer_addresses",
    "promotions",
    "riders",
    "inventory",
    "rider_shifts",
    "orders",
    "order_items",
    "payments",
    "delivery_assignments",
    "order_status_history",
    "refunds",
    "cdc_heartbeat",
)

# Debezium's own signalling table is captured (Debezium needs it in the publication) but is not data.
IGNORED: frozenset[str] = frozenset({"debezium_signal"})

# Silver tables are MERGE targets keyed by primary key; bucketing on the key keeps each MERGE's
# rewrite (merge-on-read delete files) local to a few files.
SILVER_PARTITIONING: dict[str, str] = {
    "orders": "bucket(8, order_id)",
    "order_items": "bucket(8, order_item_id)",
    "order_status_history": "bucket(8, status_event_id)",
    "payments": "bucket(4, payment_id)",
    "delivery_assignments": "bucket(4, assignment_id)",
    "inventory": "bucket(4, store_id)",
}

# Bronze change logs are append-mostly and queried by time.
BRONZE_PARTITIONING = "days(_ingested_at)"

COMMON_PROPERTIES: dict[str, str] = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
    "commit.retry.num-retries": "10",
    "commit.retry.min-wait-ms": "200",
}

SILVER_PROPERTIES: dict[str, str] = {
    **COMMON_PROPERTIES,
    # Streaming upserts: write position deletes instead of rewriting data files on every batch.
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
}

BRONZE_PROPERTIES: dict[str, str] = dict(COMMON_PROPERTIES)


def table_from_topic(topic: str) -> str:
    """``qc.commerce.orders`` -> ``orders``."""
    return topic.rsplit(".", 1)[-1]


def ordered(tables: list[str]) -> list[str]:
    rank = {name: i for i, name in enumerate(LOAD_ORDER)}
    return sorted(tables, key=lambda t: (rank.get(t, len(LOAD_ORDER)), t))
