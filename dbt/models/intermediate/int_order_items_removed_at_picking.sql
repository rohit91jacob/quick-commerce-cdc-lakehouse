-- Order lines hard-deleted when the picker found the shelf empty. A delete event only carries the key,
-- so the details of the line come from the change that created it.
with deletes as (
    select order_item_id, _source_ts as removed_at
    from {{ ref('stg_order_items__changes') }}
    where _op = 'd'
),

created as (
    select
        order_item_id, order_id, product_id, quantity, unit_price, line_total,
        row_number() over (partition by order_item_id order by _lsn) as version
    from {{ ref('stg_order_items__changes') }}
    where _op in ('r', 'c')
)

select
    c.order_item_id, c.order_id, c.product_id, c.quantity, c.unit_price, c.line_total, d.removed_at
from deletes d
join created c on c.order_item_id = d.order_item_id and c.version = 1
