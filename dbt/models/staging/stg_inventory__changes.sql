-- Includes deletes (delisting): on_hand is null for them.
select store_id, product_id, on_hand, updated_at, _op, _lsn, _source_ts
from ({{ dedupe_changes(source('bronze', 'inventory'), ['store_id', 'product_id']) }})
