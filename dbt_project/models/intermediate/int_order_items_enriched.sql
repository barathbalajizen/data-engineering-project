-- INTERMEDIATE: order items with their order's details
-- What:  One row per order item, with the order's customer, status and dates added, plus two delivery
--        metrics. fact_orders and the tests build on it.
-- Why:   "Item + its order" is needed in several places; joining once here keeps that logic in one place.
-- Columns added:
--   delivery_days     days from purchase to delivery (NULL while the order is not delivered)
--   is_late           1 if delivered after the estimated delivery date, otherwise 0
--   order_updated_at  when the order last changed. The incremental fact uses it to decide what to rebuild:
--                     when an order changes (e.g. shipped -> delivered), all of its items are rebuilt.
-- Note:  Inner join: items whose order was rejected in Silver (quarantine) are left out. A dbt test warns
--        about such items (relationships test on stg_order_items).
select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    (o.order_delivered_customer_date::date - o.order_purchase_timestamp::date)    as delivery_days,
    case when o.order_delivered_customer_date > o.order_estimated_delivery_date
         then 1 else 0 end                                                       as is_late,
    o.updated_at                                                                 as order_updated_at
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o
    on o.order_id = i.order_id
