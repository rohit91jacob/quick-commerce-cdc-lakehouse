select product_id, sku, name as product_name, brand, category_id, unit, mrp, selling_price, is_active, updated_at
from {{ source('silver', 'products') }}
where not _is_deleted
