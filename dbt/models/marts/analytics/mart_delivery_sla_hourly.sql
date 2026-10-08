-- Delivery promise performance per store, day and IST hour (delivered orders only).
select
    order_date,
    order_hour,
    store_id,
    count(*) as delivered_orders,
    max(promised_minutes) as promised_minutes_max,
    avg(delivery_minutes) as avg_delivery_minutes,
    approx_percentile(delivery_minutes, 0.5) as p50_delivery_minutes,
    approx_percentile(delivery_minutes, 0.9) as p90_delivery_minutes,
    cast(count_if(sla_met) as double) / count(*) as pct_within_promise,
    avg(case when not sla_met then delivery_minutes - promised_minutes end) as avg_minutes_late_when_breached,
    avg(minutes_to_accept) as avg_minutes_to_accept,
    avg(minutes_to_pick) as avg_minutes_to_pick,
    avg(minutes_waiting_for_rider) as avg_minutes_waiting_for_rider,
    avg(minutes_in_transit) as avg_minutes_in_transit
from {{ ref('fct_orders') }}
where status = 'delivered'
group by 1, 2, 3
