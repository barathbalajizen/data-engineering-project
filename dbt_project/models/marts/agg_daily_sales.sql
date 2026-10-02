select
    d.date_day,
    count(distinct f.order_id)  as orders,
    count(*)                    as items_sold,
    sum(f.price)                as revenue,
    sum(f.freight_value)        as freight,
    sum(f.is_late)              as late_deliveries
from {{ ref('fact_orders') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
where f.order_status <> 'canceled'
group by d.date_day
