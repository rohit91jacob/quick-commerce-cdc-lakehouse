select status_event_id, order_id, status, actor, occurred_at
from {{ source('silver', 'order_status_history') }}
where not _is_deleted
