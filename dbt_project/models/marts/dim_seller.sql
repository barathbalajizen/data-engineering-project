-- DIMENSION: seller
-- What:  One row per seller. seller_key = md5(seller_id), built the same way as product_key.
select
    md5(seller_id) as seller_key,
    seller_id,
    seller_city,
    seller_state
from {{ ref('stg_sellers') }}
