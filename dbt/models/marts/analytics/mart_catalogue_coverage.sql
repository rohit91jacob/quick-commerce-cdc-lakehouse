-- How much of the simulated store's catalogue is real: real Open Food Facts products, and real
-- Open Prices INR shelf prices, per category.
select
    category_name,
    count(*) as products,
    count_if(catalogue_source = 'open_food_facts') as real_products,
    count_if(price_source = 'open_prices') as real_priced_products,
    cast(count_if(catalogue_source = 'open_food_facts') as double) / count(*) as real_product_share
from {{ ref('dim_product') }}
group by 1
