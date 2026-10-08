-- Every captured version of a product row, in commit (LSN) order.
select product_id, mrp, selling_price, is_active, updated_at, _op, _lsn, _source_ts
from ({{ dedupe_changes(source('bronze', 'products'), ['product_id']) }})
where _op in ('r', 'c', 'u')
