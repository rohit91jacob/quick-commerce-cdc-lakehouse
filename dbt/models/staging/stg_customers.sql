select customer_id, city_id, signup_at
from {{ source('silver', 'customers') }}
where not _is_deleted
