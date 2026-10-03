select
    order_id,
    payment_sequential,
    payment_type,
    payment_value
from {{ source('staging', 'order_payments') }}
