-- DIMENSION: product
-- What:  One row per product.
-- Key:   product_key = md5(product_id), a surrogate key. A hash of the id gives the same value on every
--        run, so fact_orders can compute the same key without looking it up here.
select
    md5(product_id)     as product_key,
    product_id,
    category,
    product_weight_g
from {{ ref('stg_products') }}
