# Data dictionary

The catalog is `lakehouse` (Iceberg, JDBC catalog). Times are UTC `timestamptz`; reporting dates and hours are IST.

## Source: Postgres `commerce` schema (OLTP)

| Table | Grain / key | Notes |
|---|---|---|
| `cities` | `city_id` | |
| `dark_stores` | `store_id` | `service_radius_km`, `max_concurrent_orders` (picker capacity) and `status` change over time (SCD2 downstream) |
| `categories` | `category_id` | `is_perishable` |
| `products` | `product_id` | `mrp`, `selling_price` change daily for a few SKUs |
| `customers` | `customer_id` | pseudonymous (`Customer 000123`) |
| `customer_addresses` | `address_id` | hard-deleted when a customer removes one |
| `inventory` | (`store_id`, `product_id`) | hot rows; deleted when a SKU is delisted, re-inserted (same key) when relisted |
| `promotions` | `promo_id` | daily `FLASHyyyymmdd` promos, hard-deleted 2 days after expiry |
| `riders` | `rider_id` | `status`: offline / idle / assigned / delivering / returning |
| `rider_shifts` | `shift_id` | `ended_at` null while logged in |
| `orders` | `order_id` | lifecycle timestamps; `promised_minutes` is 10, or 15 in rain; `tip_amount` added by migration V002 |
| `order_items` | `order_item_id` | deleted when the item is missing at picking; updated on substitution |
| `order_status_history` | `status_event_id` | append-only |
| `delivery_assignments` | `assignment_id` (unique `order_id`) | |
| `payments` | `payment_id` (one per order) | initiated → captured / failed / refunded / partially_refunded |
| `refunds` | `refund_id` | reasons: order_cancelled, items_unavailable, late_delivery, payment_reversal |
| `cdc_heartbeat` | `id` | 1 = Debezium heartbeat, 2 = reconciliation fence |
| `debezium_signal` | `id` | Debezium signalling (incremental snapshots) |

## Bronze (`lakehouse.bronze.<table>`)

One row per change event, unique on (primary key, `_lsn`, `_op`). Holds the business columns, then:

| Column | Meaning |
|---|---|
| `_op` | `r` snapshot read, `c` insert, `u` update, `d` delete (business columns are null except the key) |
| `_lsn` | WAL position of the change: commit order and event identity |
| `_tx_id` | source transaction id |
| `_source_ts` | commit time in Postgres |
| `_event_ts` | time Debezium processed the change |
| `_snapshot` | Debezium snapshot marker (`true`, `first`, `last`, `false`...) |
| `_kafka_partition`, `_kafka_offset` | where the event was first read from |
| `_batch_id`, `_ingested_at` | writer micro-batch and its start time |

Partitioned by `days(_ingested_at)`. `bronze._dead_letters` holds records that could not be applied (topic, partition, offset, key, value, reason).

## Silver (`lakehouse.silver.<table>`)

Current state, one row per primary key, merge-on-read Iceberg v2. Holds the business columns, then `_op`, `_lsn`, `_tx_id`, `_source_ts`, `_ingested_at` and **`_is_deleted`**. Deleted rows are kept as tombstones; read with `WHERE NOT _is_deleted`. Hot tables are bucketed by key (see `lakehouse/tables.py`).

## dbt: `staging` (views)

`stg_<table>`: live silver rows, renamed and typed. `stg_<table>__changes` for products, dark stores, inventory and order items: bronze change logs, deduplicated per (key, LSN, op).

## dbt: `intermediate`

| Model | Grain | Logic |
|---|---|---|
| `int_inventory_stockout_periods` | store × SKU × stock-out period | gaps-and-islands over the inventory change log; delisting ends a period |
| `int_order_items_removed_at_picking` | removed order line | delete events joined to the line's creating event |

## dbt: `gold`

| Model | Grain | Key measures |
|---|---|---|
| `dim_store` | store | current attributes, city |
| `dim_store_history` | store × version | SCD2 of radius, capacity and status (`valid_from`, `valid_to`, `is_current`) |
| `dim_product` | product | category, prices, `discount_rate` |
| `dim_product_price_history` | product × version | SCD2 of MRP and selling price, from the change log |
| `dim_customer` | customer | first and last order, `cohort_week`, lifetime net revenue |
| `fct_orders` | order | stage durations, `delivery_minutes`, `sla_met`, money, refunds, `net_revenue`; incremental (delete+insert) |
| `fct_order_items` | surviving order line | quantity, prices, substitution flag; incremental by order |
| `mart_order_funnel_daily` | store × IST day | orders per stage, cancellations by reason, placed→delivered rate |
| `mart_delivery_sla_hourly` | store × IST day × hour | P50/P90 delivery minutes, share within the promise, stage averages |
| `mart_inventory_health_daily` | store × IST day | stock-out events, stock-out rate, estimated lost units and sales, lines removed at picking |
| `mart_basket_daily` | city × IST day | GMV, discounts, refunds, net revenue, AOV, lines and units per order |
| `mart_category_mix_daily` | category × IST day | units, revenue, revenue share |
| `mart_customer_retention_weekly` | cohort week × week number | cohort size, active customers, retention rate |
| `mart_rider_utilisation_daily` | rider × IST day | logged-in vs busy minutes, utilisation, deliveries per hour |
| `mart_promo_effectiveness` | promo code | orders, first orders, discount cost, basket uplift vs same-day non-promo orders |

Estimated lost sales = stock-out minutes × the SKU's observed sell rate at that store × its average selling price.
