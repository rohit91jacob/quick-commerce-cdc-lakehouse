-- Stock-outs and the sales they probably cost, per store and day.
-- Estimated lost sales = minutes out of stock x the SKU's observed sell rate at that store x its average price.
with sell_rate as (
    select
        store_id,
        product_id,
        sum(quantity) as units_sold,
        sum(line_total) / sum(quantity) as avg_unit_price,
        {{ minutes_between('min(o.placed_at)', 'max(o.placed_at)') }} as observed_minutes
    from {{ ref('fct_order_items') }} i
    join {{ ref('fct_orders') }} o using (order_id, store_id)
    where o.status = 'delivered'
    group by 1, 2
),

stockouts as (
    select
        s.store_id,
        s.product_id,
        {{ local_date('s.started_at') }} as stockout_date,
        s.stockout_minutes,
        s.is_ongoing,
        r.units_sold / nullif(r.observed_minutes, 0) as units_per_minute,
        r.avg_unit_price
    from {{ ref('int_inventory_stockout_periods') }} s
    left join sell_rate r on r.store_id = s.store_id and r.product_id = s.product_id
),

removed as (
    select o.store_id, {{ local_date('r.removed_at') }} as removal_date, count(*) as lines_removed_at_picking,
           sum(r.line_total) as value_removed_at_picking
    from {{ ref('int_order_items_removed_at_picking') }} r
    join {{ ref('stg_orders') }} o on o.order_id = r.order_id
    group by 1, 2
),

daily as (
    select
        store_id,
        stockout_date as activity_date,
        count(*) as stockout_events,
        count(distinct product_id) as skus_stocked_out,
        count_if(is_ongoing) as stockouts_ongoing,
        sum(stockout_minutes) as stockout_minutes,
        sum(stockout_minutes * coalesce(units_per_minute, 0)) as estimated_lost_units,
        sum(stockout_minutes * coalesce(units_per_minute, 0) * coalesce(avg_unit_price, 0)) as estimated_lost_sales
    from stockouts
    group by 1, 2
),

catalogue as (
    select store_id, count(*) as skus_listed, count_if(on_hand = 0) as skus_at_zero_now,
           count_if(on_hand <= reorder_point) as skus_below_reorder_point
    from {{ ref('stg_inventory') }}
    group by 1
)

select
    d.store_id,
    d.activity_date,
    c.skus_listed,
    c.skus_at_zero_now,
    c.skus_below_reorder_point,
    d.stockout_events,
    d.skus_stocked_out,
    d.stockouts_ongoing,
    d.stockout_minutes,
    cast(d.skus_stocked_out as double) / nullif(c.skus_listed, 0) as stockout_rate,
    d.estimated_lost_units,
    cast(d.estimated_lost_sales as decimal(14, 2)) as estimated_lost_sales,
    coalesce(r.lines_removed_at_picking, 0) as lines_removed_at_picking,
    coalesce(r.value_removed_at_picking, 0) as value_removed_at_picking
from daily d
left join catalogue c on c.store_id = d.store_id
left join removed r on r.store_id = d.store_id and r.removal_date = d.activity_date
