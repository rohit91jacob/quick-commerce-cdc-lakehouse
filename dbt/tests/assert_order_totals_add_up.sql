-- total = subtotal - discount + delivery fee, and no order refunds more than it charged.
select order_id, subtotal, discount, delivery_fee, total, refunded_amount
from {{ ref('fct_orders') }}
where total <> subtotal - discount + delivery_fee
    or refunded_amount > total
