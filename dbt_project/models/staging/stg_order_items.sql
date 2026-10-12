-- STAGING: order items
-- What:  One row per product line in an order. order_item_id numbers the lines of one order (1, 2, 3 ...),
--        so the unique key is (order_id, order_item_id).
-- Note:  price is per item; freight_value is the shipping cost charged for that item.
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
from {{ source('staging', 'order_items') }}
