select
    p.category,
    count(distinct f.order_id)  as orders,
    sum(f.price)                as revenue,
    avg(f.delivery_days)        as avg_delivery_days
from {{ ref('fact_orders') }} f
join {{ ref('dim_product') }} p on p.product_key = f.product_key
where f.order_status <> 'canceled'
group by p.category
