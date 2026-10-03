select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    updated_at
from {{ source('staging', 'customers') }}
