-- Type-2 history of the operational attributes of each dark store (catchment, picker capacity, status).
with ordered as (
    select
        store_id, service_radius_km, max_concurrent_orders, status, updated_at, _lsn,
        lag(service_radius_km) over (partition by store_id order by _lsn) as previous_radius,
        lag(max_concurrent_orders) over (partition by store_id order by _lsn) as previous_capacity,
        lag(status) over (partition by store_id order by _lsn) as previous_status
    from {{ ref('stg_dark_stores__changes') }}
),

versions as (
    select store_id, service_radius_km, max_concurrent_orders, status, updated_at as valid_from, _lsn
    from ordered
    where previous_status is null
        or service_radius_km <> previous_radius
        or max_concurrent_orders <> previous_capacity
        or status <> previous_status
)

select
    store_id,
    row_number() over (partition by store_id order by _lsn) as version,
    service_radius_km,
    max_concurrent_orders,
    status,
    valid_from,
    lead(valid_from) over (partition by store_id order by _lsn) as valid_to,
    lead(valid_from) over (partition by store_id order by _lsn) is null as is_current,
    _lsn as source_lsn
from versions
