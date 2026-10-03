-- Payments with their order's purchase date. Grain: one row per payment.
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
