select
    order_item_id, order_id, product_id, quantity, unit_price, mrp, line_total, substituted_for_product_id,
    updated_at, _ingested_at
from {{ source('silver', 'order_items') }}
where not _is_deleted
