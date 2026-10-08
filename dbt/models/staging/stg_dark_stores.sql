select
    store_id, city_id, name as store_name, lat, lng, service_radius_km, max_concurrent_orders, status, opened_at,
    updated_at
from {{ source('silver', 'dark_stores') }}
where not _is_deleted
