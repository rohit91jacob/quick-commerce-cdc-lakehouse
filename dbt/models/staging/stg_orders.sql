select
    order_id, customer_id, store_id, address_id, distance_km, status, promo_code, items_count, subtotal, discount,
    delivery_fee, total, payment_method, promised_minutes, placed_at, accepted_at, picking_started_at, packed_at,
    dispatched_at, delivered_at, cancelled_at, cancel_reason,
    {{ optional_column(source('silver', 'orders'), 'tip_amount', 'decimal(8,2)') }},
    updated_at, _ingested_at
from {{ source('silver', 'orders') }}
where not _is_deleted
