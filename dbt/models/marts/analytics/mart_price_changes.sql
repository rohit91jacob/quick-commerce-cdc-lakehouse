-- REAL: every shelf-price change between consecutive Open Prices observations of the same product
-- at the same shop (and currency). One row per observation that has a predecessor.
with obs as (
    select
        p.price_id, p.product_code, p.location_id, p.currency, p.price, p.is_discounted, p.observed_on,
        p.source_created_at,
        lag(p.price) over w as previous_price,
        lag(p.observed_on) over w as previous_observed_on
    from {{ ref('stg_market_prices') }} p
    where p.product_code is not null and p.location_id is not null
    window w as (partition by p.product_code, p.location_id, p.currency order by p.observed_on, p.source_created_at, p.price_id)
)
select
    o.price_id, o.product_code, mp.product_name, mp.brand, coalesce(mp.category_tag, 'en:unknown') as category_tag,
    o.location_id, l.shop_name, l.city, l.country_code, o.currency,
    o.previous_observed_on, o.observed_on, o.previous_price, o.price, o.is_discounted,
    o.price - o.previous_price as price_change,
    cast((o.price - o.previous_price) / o.previous_price as double) as change_pct
from obs o
left join (
    select product_code, max(product_name) as product_name, max(brand) as brand, max(category_tag) as category_tag
    from {{ ref('stg_market_products') }} group by 1
) mp on mp.product_code = o.product_code
left join {{ ref('stg_market_locations') }} l on l.location_id = o.location_id
where o.previous_price is not null
