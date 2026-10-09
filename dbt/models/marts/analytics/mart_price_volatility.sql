-- REAL: per product-shop series, how often and how much the shelf price moves. A series is flagged
-- volatile when it changed at least twice and either swung by more than 30% or its log changes have
-- a standard deviation above 0.15.
with changes as (
    select product_code, location_id, currency, change_pct, ln(1 + change_pct) as log_change
    from {{ ref('mart_price_changes') }}
),
observations as (
    select product_code, location_id, currency, count(*) as observations
    from {{ ref('stg_market_prices') }}
    where product_code is not null and location_id is not null
    group by 1, 2, 3
)
select
    o.product_code, o.location_id, o.currency, o.observations,
    count_if(c.change_pct <> 0) as price_changes,
    coalesce(avg(abs(c.change_pct)), 0) as avg_abs_change_pct,
    coalesce(max(abs(c.change_pct)), 0) as max_abs_change_pct,
    coalesce(stddev_samp(c.log_change), 0) as log_change_stddev,
    count_if(c.change_pct <> 0) >= 2
        and (coalesce(max(abs(c.change_pct)), 0) > 0.30 or coalesce(stddev_samp(c.log_change), 0) > 0.15)
        as is_volatile
from observations o
left join changes c
    on c.product_code = o.product_code and c.location_id = o.location_id and c.currency = o.currency
group by 1, 2, 3, 4
