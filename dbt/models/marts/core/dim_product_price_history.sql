-- Type-2 history of product prices, built from the CDC change log rather than periodic snapshots,
-- so every intermediate price is captured. Ordered by LSN (commit order), so late or duplicated
-- delivery of change events cannot reorder versions.
with ordered as (
    select
        product_id, mrp, selling_price, updated_at, _lsn,
        lag(mrp) over (partition by product_id order by _lsn) as previous_mrp,
        lag(selling_price) over (partition by product_id order by _lsn) as previous_price
    from {{ ref('stg_products__changes') }}
),

versions as (
    -- updates that only touch other columns are not new price versions
    select product_id, mrp, selling_price, updated_at as valid_from, _lsn
    from ordered
    where previous_mrp is null or mrp <> previous_mrp or selling_price <> previous_price
)

select
    product_id,
    row_number() over (partition by product_id order by _lsn) as version,
    mrp,
    selling_price,
    valid_from,
    lead(valid_from) over (partition by product_id order by _lsn) as valid_to,
    lead(valid_from) over (partition by product_id order by _lsn) is null as is_current,
    _lsn as source_lsn
from versions
