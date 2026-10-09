select
    product_id, sku, name as product_name, brand, category_id, unit, mrp, selling_price, is_active, updated_at,
    -- V003 provenance (defaults until the migration has flowed through CDC)
    {{ optional_column(source('silver', 'products'), 'off_code', 'varchar') }},
    coalesce({{ optional_value(source('silver', 'products'), 'catalogue_source', 'varchar') }}, 'simulated')
        as catalogue_source,
    coalesce({{ optional_value(source('silver', 'products'), 'price_source', 'varchar') }}, 'simulated')
        as price_source,
    {{ optional_column(source('silver', 'products'), 'nutriscore_grade', 'varchar') }}
from {{ source('silver', 'products') }}
where not _is_deleted
