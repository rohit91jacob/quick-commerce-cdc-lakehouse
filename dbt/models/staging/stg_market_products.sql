-- REAL: Open Food Facts products referenced by Open Prices.
select
    market_product_id, code as product_code, name as product_name, brand, category_tag, store_category,
    quantity, quantity_unit, nutriscore_grade
from {{ source('silver', 'market_products') }}
where not _is_deleted
