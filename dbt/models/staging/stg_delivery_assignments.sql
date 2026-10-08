select assignment_id, order_id, rider_id, assigned_at, picked_up_at, delivered_at, distance_km
from {{ source('silver', 'delivery_assignments') }}
where not _is_deleted
