-- Weekly cohort retention: share of customers (by week of first delivered order) ordering again N weeks later.
with activity as (
    select distinct
        c.cohort_week,
        o.customer_id,
        date_diff('week', c.cohort_week, date_trunc('week', o.order_date)) as week_number
    from {{ ref('fct_orders') }} o
    join {{ ref('dim_customer') }} c on c.customer_id = o.customer_id
    where o.status = 'delivered'
),

cohorts as (
    select cohort_week, count(distinct customer_id) as cohort_size
    from activity
    group by 1
)

select
    a.cohort_week,
    a.week_number,
    c.cohort_size,
    count(distinct a.customer_id) as active_customers,
    cast(count(distinct a.customer_id) as double) / c.cohort_size as retention_rate
from activity a
join cohorts c on c.cohort_week = a.cohort_week
group by 1, 2, 3
