select refund_id, order_id, payment_id, amount, reason, status as refund_status, created_at, _ingested_at
from {{ source('silver', 'refunds') }}
where not _is_deleted
