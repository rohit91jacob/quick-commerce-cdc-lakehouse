select rider_id, store_id, vehicle_type, status, joined_at
from {{ source('silver', 'riders') }}
where not _is_deleted
