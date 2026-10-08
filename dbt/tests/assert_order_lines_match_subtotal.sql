-- The surviving order lines must add up to the order subtotal (lines removed at picking included).
-- Orders cancelled because every line was unavailable keep their lines, so they are checked too.
with lines as (
    select order_id, sum(line_total) as line_total
    from {{ ref('fct_order_items') }}
    group by 1
)

select o.order_id, o.subtotal, l.line_total
from {{ ref('fct_orders') }} o
left join lines l on l.order_id = o.order_id
where coalesce(l.line_total, 0) <> o.subtotal
