-- Promo code performance: reach, cost, basket uplift and new-customer acquisition.
with orders as (
    select
        *,
        row_number() over (partition by customer_id order by placed_at) = 1 as is_first_order
    from {{ ref('fct_orders') }}
    where status = 'delivered'
),

baseline as (
    select order_date, avg(subtotal) as avg_basket_without_promo
    from orders
    where not used_promo
    group by 1
)

select
    o.promo_code,
    count(*) as orders,
    count(distinct o.customer_id) as customers,
    count_if(o.is_first_order) as first_orders,
    sum(o.subtotal) as gross_merchandise_value,
    sum(o.discount) as discount_cost,
    avg(o.subtotal) as avg_basket_with_promo,
    avg(b.avg_basket_without_promo) as avg_basket_without_promo_same_days,
    avg(o.subtotal) - avg(b.avg_basket_without_promo) as basket_uplift,
    cast(sum(o.discount) as double) / nullif(sum(o.subtotal), 0) as discount_rate
from orders o
left join baseline b on b.order_date = o.order_date
where o.used_promo
group by 1
