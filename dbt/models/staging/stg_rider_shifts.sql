select shift_id, rider_id, store_id, started_at, ended_at
from {{ source('silver', 'rider_shifts') }}
where not _is_deleted
