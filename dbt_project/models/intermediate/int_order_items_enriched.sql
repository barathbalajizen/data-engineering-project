-- Order items with their order's attributes and delivery metrics. Grain: one row per order item.
-- order_updated_at drives the incremental facts: an item is reprocessed whenever its order changes.
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
