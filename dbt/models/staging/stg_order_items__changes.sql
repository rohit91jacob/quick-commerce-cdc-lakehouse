select order_item_id, order_id, product_id, quantity, unit_price, line_total, updated_at, _op, _lsn, _source_ts
from ({{ dedupe_changes(source('bronze', 'order_items'), ['order_item_id']) }})
