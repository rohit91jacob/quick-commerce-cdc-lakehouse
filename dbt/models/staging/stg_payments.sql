select payment_id, order_id, method, amount, status as payment_status, created_at, updated_at, _ingested_at
from {{ source('silver', 'payments') }}
where not _is_deleted
