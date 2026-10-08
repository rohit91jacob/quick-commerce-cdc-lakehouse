-- Order funnel per store and day: how many orders reached each stage, and where they were lost.
select
    order_date,
    store_id,
    count(*) as orders_placed,
    count(accepted_at) as orders_accepted,
    count(picking_started_at) as orders_picking_started,
    count(packed_at) as orders_packed,
    count(dispatched_at) as orders_dispatched,
    count_if(status = 'delivered') as orders_delivered,
    count_if(status = 'cancelled') as orders_cancelled,
    count_if(cancel_reason = 'customer_cancelled') as cancelled_by_customer,
    count_if(cancel_reason = 'payment_failed') as cancelled_payment_failed,
    count_if(cancel_reason = 'items_unavailable') as cancelled_items_unavailable,
    count_if(cancel_reason = 'system_recovery') as cancelled_system_recovery,
    count_if(status not in ('delivered', 'cancelled')) as orders_in_flight,
    cast(count_if(status = 'delivered') as double) / count(*) as placed_to_delivered_rate
from {{ ref('fct_orders') }}
group by 1, 2
