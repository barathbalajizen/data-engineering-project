select
    md5(p.order_id || '-' || p.payment_sequential::text)     as payment_key,
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_value,
    to_char(o.order_purchase_timestamp, 'YYYYMMDD')::int     as date_key
from {{ source('staging', 'order_payments') }} p
join {{ source('staging', 'orders') }} o on o.order_id = p.order_id
