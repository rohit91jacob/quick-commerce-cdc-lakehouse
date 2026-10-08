-- Gaps-and-islands over the inventory change log: one row per continuous period at zero stock.
-- A delete (SKU delisted) ends tracking; a later relist starts a new history for the same key.
with events as (
    select
        store_id, product_id, _lsn,
        coalesce(updated_at, _source_ts) as changed_at,
        case when _op = 'd' then 'delisted' when on_hand = 0 then 'out' else 'in' end as state
    from {{ ref('stg_inventory__changes') }}
),

changes as (
    select
        *,
        case
            when lag(state) over (partition by store_id, product_id order by _lsn) = state then 0 else 1
        end as starts_island
    from events
),

islands as (
    select
        *,
        sum(starts_island) over (
            partition by store_id, product_id order by _lsn rows between unbounded preceding and current row
        ) as island
    from changes
),

spans as (
    select store_id, product_id, island, min(state) as state, min(changed_at) as started_at
    from islands
    group by 1, 2, 3
),

bounded as (
    select
        *,
        lead(started_at) over (partition by store_id, product_id order by island) as ended_at
    from spans
)

select
    store_id,
    product_id,
    started_at,
    ended_at,
    ended_at is null as is_ongoing,
    {{ minutes_between('started_at', 'coalesce(ended_at, current_timestamp)') }} as stockout_minutes
from bounded
where state = 'out'
