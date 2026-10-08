with delivered as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(*) as delivered_orders,
        sum(net_revenue) as lifetime_net_revenue
    from {{ ref('fct_orders') }}
    where status = 'delivered'
    group by 1
)

select
    c.customer_id,
    c.city_id,
    c.signup_at,
    d.first_order_date,
    date_trunc('week', d.first_order_date) as cohort_week,
    d.last_order_date,
    coalesce(d.delivered_orders, 0) as delivered_orders,
    coalesce(d.lifetime_net_revenue, 0) as lifetime_net_revenue
from {{ ref('stg_customers') }} c
left join delivered d on d.customer_id = c.customer_id
