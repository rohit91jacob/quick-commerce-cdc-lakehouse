select promo_id, code as promo_code, discount_type, discount_value, max_discount, min_order_value, starts_at, ends_at
from {{ source('silver', 'promotions') }}
where not _is_deleted
