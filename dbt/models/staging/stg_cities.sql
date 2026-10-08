select city_id, name as city_name, state, center_lat, center_lng
from {{ source('silver', 'cities') }}
where not _is_deleted
