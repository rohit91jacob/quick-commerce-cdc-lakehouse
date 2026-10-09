-- REAL: how much the same product's shelf price differs between shops in the same ISO week.
with weekly as (
    select
        p.product_code, p.currency, date_trunc('week', p.observed_on) as week_start, p.location_id,
        max_by(p.price, p.source_created_at) as price
    from {{ ref('stg_market_prices') }} p
    where p.product_code is not null and p.location_id is not null
    group by 1, 2, 3, 4
)
select
    product_code, currency, week_start,
    count(*) as shops,
    min(price) as min_price,
    max(price) as max_price,
    cast(avg(price) as double) as avg_price,
    cast(stddev_samp(price) / nullif(avg(price), 0) as double) as coefficient_of_variation,
    cast(max(price) / nullif(min(price), 0) as double) as max_to_min_ratio
from weekly
group by 1, 2, 3
having count(*) >= 2
