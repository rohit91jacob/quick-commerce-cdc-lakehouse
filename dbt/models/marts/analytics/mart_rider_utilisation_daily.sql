-- Rider productivity per day: logged-in time vs time spent on deliveries.
with shifts as (
    select
        rider_id,
        store_id,
        {{ local_date('started_at') }} as shift_date,
        sum({{ minutes_between('started_at', 'coalesce(ended_at, current_timestamp)') }}) as logged_in_minutes
    from {{ ref('stg_rider_shifts') }}
    group by 1, 2, 3
),

trips as (
    select
        rider_id,
        {{ local_date('assigned_at') }} as trip_date,
        count(*) as assignments,
        count(delivered_at) as deliveries,
        sum({{ minutes_between('assigned_at', 'delivered_at') }}) as busy_minutes,
        avg(distance_km) as avg_distance_km
    from {{ ref('stg_delivery_assignments') }}
    group by 1, 2
)

select
    s.shift_date,
    s.store_id,
    s.rider_id,
    s.logged_in_minutes,
    coalesce(t.assignments, 0) as assignments,
    coalesce(t.deliveries, 0) as deliveries,
    coalesce(t.busy_minutes, 0) as busy_minutes,
    t.avg_distance_km,
    least(1.0, coalesce(t.busy_minutes, 0) / nullif(s.logged_in_minutes, 0)) as utilisation,
    coalesce(t.deliveries, 0) / nullif(s.logged_in_minutes / 60.0, 0) as deliveries_per_hour
from shifts s
left join trips t on t.rider_id = s.rider_id and t.trip_date = s.shift_date
