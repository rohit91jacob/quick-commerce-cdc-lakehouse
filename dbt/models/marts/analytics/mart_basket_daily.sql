-- Basket economics per city and day (delivered orders). Orders are simulated; their line prices are
-- the catalogue's selling prices, which follow real Open Prices INR shelf prices where one exists.
-- real_price_value_share is the share of line value on products whose price is real (provenance as
-- of today, not as of the order).
with lines as (
    select
        i.order_id,
        sum(i.line_total) as line_value,
        sum(case when p.price_source = 'open_prices' then i.line_total else 0 end) as real_priced_value,
        sum(case when p.catalogue_source = 'open_food_facts' then i.line_total else 0 end) as real_product_value
    from {{ ref('fct_order_items') }} i
    join {{ ref('dim_product') }} p on p.product_id = i.product_id
    group by 1
)
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
    cast(count_if(o.substituted_lines > 0) as double) / count(*) as share_orders_with_substitution,
    cast(sum(l.real_product_value) as double) / nullif(sum(l.line_value), 0) as real_product_value_share,
    cast(sum(l.real_priced_value) as double) / nullif(sum(l.line_value), 0) as real_price_value_share
from {{ ref('fct_orders') }} o
join {{ ref('dim_store') }} s on s.store_id = o.store_id
left join lines l on l.order_id = o.order_id
where o.status = 'delivered'
group by 1, 2
