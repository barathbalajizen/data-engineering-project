select
    md5(product_id)                              as product_key,
    product_id,
    coalesce(product_category_name, 'unknown')   as category,
    product_weight_g
from {{ source('staging', 'products') }}
