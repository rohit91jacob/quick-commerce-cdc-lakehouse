{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
    )
}}
-- One row per order. Incremental: rebuilds orders whose order row, lines, payment or refunds changed
-- within the lookback window of the latest load, so late refunds and picking changes are picked up.

{% if is_incremental() %}
{% set since %}
    (select max(_ingested_at) - interval '{{ var("incremental_lookback_hours") }}' hour from {{ this }})
{% endset %}
{% endif %}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where _ingested_at >= {{ since }}
        or order_id in (select order_id from {{ ref('stg_order_items') }} where _ingested_at >= {{ since }})
        or order_id in (select order_id from {{ ref('stg_refunds') }} where _ingested_at >= {{ since }})
        or order_id in (select order_id from {{ ref('stg_payments') }} where _ingested_at >= {{ since }})
    {% endif %}
),

lines as (
    select
        order_id,
        count(*) as line_count,
        sum(quantity) as units,
        count_if(substituted_for_product_id is not null) as substituted_lines
    from {{ ref('stg_order_items') }}
    group by 1
),

refunds as (
    select order_id, sum(amount) as refunded_amount, count(*) as refund_count
    from {{ ref('stg_refunds') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.store_id,
    a.rider_id,
    o.status,
    o.cancel_reason,
    o.payment_method,
    p.payment_status,
    o.promo_code,
    o.promo_code is not null as used_promo,
    {{ local_date('o.placed_at') }} as order_date,
    hour({{ local_ts('o.placed_at') }}) as order_hour,
    o.placed_at,
    o.accepted_at,
    o.picking_started_at,
    o.packed_at,
    o.dispatched_at,
    o.delivered_at,
    o.cancelled_at,
    o.promised_minutes,
    o.distance_km,
    coalesce(l.line_count, 0) as line_count,
    coalesce(l.units, 0) as units,
    coalesce(l.substituted_lines, 0) as substituted_lines,
    o.subtotal,
    o.discount,
    o.delivery_fee,
    o.total,
    o.tip_amount,
    coalesce(r.refunded_amount, 0) as refunded_amount,
    o.total - coalesce(r.refunded_amount, 0) as net_revenue,
    {{ minutes_between('o.placed_at', 'o.accepted_at') }} as minutes_to_accept,
    {{ minutes_between('o.picking_started_at', 'o.packed_at') }} as minutes_to_pick,
    {{ minutes_between('o.packed_at', 'o.dispatched_at') }} as minutes_waiting_for_rider,
    {{ minutes_between('o.dispatched_at', 'o.delivered_at') }} as minutes_in_transit,
    {{ minutes_between('o.placed_at', 'o.delivered_at') }} as delivery_minutes,
    case
        when o.status = 'delivered' then {{ minutes_between('o.placed_at', 'o.delivered_at') }} <= o.promised_minutes
    end as sla_met,
    o._ingested_at
from orders o
left join lines l on l.order_id = o.order_id
left join refunds r on r.order_id = o.order_id
left join {{ ref('stg_delivery_assignments') }} a on a.order_id = o.order_id
left join {{ ref('stg_payments') }} p on p.order_id = o.order_id
