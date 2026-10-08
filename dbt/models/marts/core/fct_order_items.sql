{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}
-- One row per surviving order line. Incremental by order: all lines of a touched order are replaced,
-- so lines deleted at the source (removed at picking) disappear here too.
with touched as (
    select distinct order_id from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where _ingested_at >= (
        select max(_ingested_at) - interval '{{ var("incremental_lookback_hours") }}' hour from {{ this }}
    )
    {% endif %}
)

select
    i.order_item_id,
    i.order_id,
    o.store_id,
    {{ local_date('o.placed_at') }} as order_date,
    o.status as order_status,
    i.product_id,
    p.category_id,
    i.quantity,
    i.unit_price,
    i.mrp,
    i.line_total,
    i.substituted_for_product_id,
    i.substituted_for_product_id is not null as is_substitution,
    i._ingested_at
from {{ ref('stg_order_items') }} i
join touched t on t.order_id = i.order_id
join {{ ref('stg_orders') }} o on o.order_id = i.order_id
join {{ ref('stg_products') }} p on p.product_id = i.product_id
