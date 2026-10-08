-- What sells: units, revenue and revenue share per category and day (delivered orders).
with lines as (
    select i.order_date, p.category_name, i.quantity, i.line_total
    from {{ ref('fct_order_items') }} i
    join {{ ref('dim_product') }} p on p.product_id = i.product_id
    where i.order_status = 'delivered'
)

select
    order_date,
    category_name,
    sum(quantity) as units,
    sum(line_total) as revenue,
    cast(sum(line_total) as double) / sum(sum(line_total)) over (partition by order_date) as revenue_share
from lines
group by 1, 2
