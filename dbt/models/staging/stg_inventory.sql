select store_id, product_id, on_hand, reorder_point, max_stock, last_restocked_at, updated_at
from {{ source('silver', 'inventory') }}
where not _is_deleted
