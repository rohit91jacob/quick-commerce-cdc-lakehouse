-- REAL: Open Prices shelf-price observations.
select
    price_id, market_product_id, location_id, product_code, category_tag, price, currency, is_discounted,
    price_without_discount, price_per, observed_on, source_created_at, _ingested_at
from {{ source('silver', 'market_prices') }}
where not _is_deleted
