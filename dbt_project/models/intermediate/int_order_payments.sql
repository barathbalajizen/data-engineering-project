-- INTERMEDIATE: payments with their order's details
-- What:  One row per payment, with the order's purchase time (for the date key) and order_updated_at
--        (so a payment is rebuilt in fact_payments whenever its order changes).
select
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_value,
    o.order_purchase_timestamp,
    o.updated_at                                                                 as order_updated_at
from {{ ref('stg_order_payments') }} p
join {{ ref('stg_orders') }} o
    on o.order_id = p.order_id
