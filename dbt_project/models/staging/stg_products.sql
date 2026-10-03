select
    product_id,
    coalesce(product_category_name, 'unknown')   as category,
    product_weight_g
from {{ source('staging', 'products') }}
