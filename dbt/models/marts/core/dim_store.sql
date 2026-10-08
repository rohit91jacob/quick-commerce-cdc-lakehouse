select
    s.store_id, s.store_name, s.city_id, c.city_name, c.state, s.lat, s.lng, s.service_radius_km,
    s.max_concurrent_orders, s.status, s.opened_at
from {{ ref('stg_dark_stores') }} s
join {{ ref('stg_cities') }} c on c.city_id = s.city_id
