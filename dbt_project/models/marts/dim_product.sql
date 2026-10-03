select
    md5(product_id)     as product_key,
    product_id,
    category,
    product_weight_g
from {{ ref('stg_products') }}
