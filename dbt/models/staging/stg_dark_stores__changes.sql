select store_id, service_radius_km, max_concurrent_orders, status, updated_at, _op, _lsn, _source_ts
from ({{ dedupe_changes(source('bronze', 'dark_stores'), ['store_id']) }})
where _op in ('r', 'c', 'u')
