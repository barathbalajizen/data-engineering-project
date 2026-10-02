-- Grain: one row per order item. Customer is joined to the dimension version
-- that was valid at purchase time (point-in-time SCD2 join).
select
    md5(i.order_id || '-' || i.order_item_id::text)              as order_item_key,
    i.order_id,
    i.order_item_id,
    c.customer_key,
    md5(i.product_id)                                            as product_key,
    md5(i.seller_id)                                             as seller_key,
    to_char(o.order_purchase_timestamp, 'YYYYMMDD')::int         as date_key,
    o.order_status,
    o.order_purchase_timestamp,
    i.price,
    i.freight_value,
    (o.order_delivered_customer_date::date
        - o.order_purchase_timestamp::date)                      as delivery_days,
    case when o.order_delivered_customer_date > o.order_estimated_delivery_date
         then 1 else 0 end                                       as is_late
from {{ source('staging', 'order_items') }} i
join {{ source('staging', 'orders') }} o
    on o.order_id = i.order_id
left join {{ ref('dim_customer') }} c
    on  c.customer_id = o.customer_id
    and o.order_purchase_timestamp >= c.valid_from
    and o.order_purchase_timestamp <  c.valid_to
