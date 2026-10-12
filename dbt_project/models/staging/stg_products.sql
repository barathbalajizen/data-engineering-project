-- STAGING: products
-- What:  One row per product. A missing category becomes 'unknown' so the product still shows up in
--        category reports instead of disappearing into a NULL group.
select
    product_id,
    coalesce(product_category_name, 'unknown')   as category,
    product_weight_g
from {{ source('staging', 'products') }}
