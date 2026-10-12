-- STAGING: sellers
-- What:  One row per seller.
select
    seller_id,
    seller_city,
    seller_state
from {{ source('staging', 'sellers') }}
