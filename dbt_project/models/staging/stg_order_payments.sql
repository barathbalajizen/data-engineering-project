-- STAGING: order payments
-- What:  One row per payment. One order can be paid in several parts (for example part voucher, part credit
--        card); payment_sequential numbers them (1, 2 ...), so the unique key is (order_id, payment_sequential).
select
    order_id,
    payment_sequential,
    payment_type,
    payment_value
from {{ source('staging', 'order_payments') }}
