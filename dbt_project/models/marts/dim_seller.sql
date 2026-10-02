select
    md5(seller_id) as seller_key,
    seller_id,
    seller_city,
    seller_state
from {{ source('staging', 'sellers') }}
