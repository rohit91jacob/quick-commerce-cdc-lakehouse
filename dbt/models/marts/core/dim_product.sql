select
    p.product_id, p.sku, p.product_name, p.brand, p.unit, p.category_id, c.category_name, c.is_perishable,
    p.mrp, p.selling_price, cast(1 - p.selling_price / p.mrp as decimal(6, 4)) as discount_rate, p.is_active,
    p.off_code, p.catalogue_source, p.price_source, p.nutriscore_grade
from {{ ref('stg_products') }} p
join {{ ref('stg_categories') }} c on c.category_id = p.category_id
