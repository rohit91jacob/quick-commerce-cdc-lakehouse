-- REAL: a Jevons (geometric-mean) price index per Open Food Facts category and currency.
-- Each product-shop series is rebased to its first observation (= 100); the index for a day is the
-- geometric mean of the latest relative of every series observed up to that day.
with series as (
    select
        p.product_code, p.location_id, p.currency, p.observed_on, p.price,
        coalesce(mp.category_tag, 'en:unknown') as category_tag,
        first_value(p.price) over (
            partition by p.product_code, p.location_id, p.currency order by p.observed_on, p.source_created_at
        ) as base_price,
        row_number() over (
            partition by p.product_code, p.location_id, p.currency, p.observed_on order by p.source_created_at desc
        ) as rn
    from {{ ref('stg_market_prices') }} p
    left join (
        select product_code, max(category_tag) as category_tag from {{ ref('stg_market_products') }} group by 1
    ) mp on mp.product_code = p.product_code
    where p.product_code is not null and p.location_id is not null
),
daily as (
    select category_tag, currency, observed_on, product_code, location_id,
           ln(cast(price as double) / cast(base_price as double)) as log_relative
    from series where rn = 1
)
select
    category_tag, currency, observed_on,
    count(*) as series_observed,
    100 * exp(avg(log_relative)) as price_index
from daily
group by 1, 2, 3
