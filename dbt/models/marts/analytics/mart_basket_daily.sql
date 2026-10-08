-- Basket economics per city and day (delivered orders).
select
    o.order_date,
    s.city_name,
    count(*) as delivered_orders,
    sum(o.subtotal) as gross_merchandise_value,
    sum(o.discount) as discounts,
    sum(o.delivery_fee) as delivery_fees,
    sum(o.refunded_amount) as refunds,
    sum(o.net_revenue) as net_revenue,
    sum(coalesce(o.tip_amount, 0)) as tips,
    avg(o.total) as average_order_value,
    avg(o.line_count) as avg_lines_per_order,
    avg(o.units) as avg_units_per_order,
    cast(sum(o.discount) as double) / nullif(sum(o.subtotal), 0) as discount_share,
    cast(count_if(o.substituted_lines > 0) as double) / count(*) as share_orders_with_substitution
from {{ ref('fct_orders') }} o
join {{ ref('dim_store') }} s on s.store_id = o.store_id
where o.status = 'delivered'
group by 1, 2
