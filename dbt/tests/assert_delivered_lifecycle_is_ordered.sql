-- Every delivered order went through each stage in order.
select order_id
from {{ ref('fct_orders') }}
where status = 'delivered'
    and not (
        placed_at <= accepted_at
        and accepted_at <= picking_started_at
        and picking_started_at <= packed_at
        and packed_at <= dispatched_at
        and dispatched_at <= delivered_at
    )
