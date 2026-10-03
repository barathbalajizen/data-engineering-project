-- One row per month: orders, customers (new vs returning), revenue and average order value.
-- A customer is a person (customer_unique_id): Olist gives every order its own customer_id.
-- Revenue = item prices of non-canceled orders (same definition as agg_daily_sales; freight separate).
with orders as (
    select
        f.order_id,
        c.customer_unique_id,
        min(f.order_purchase_timestamp)  as purchased_at,
        count(*)                         as items,
        sum(f.price)                     as revenue,
        sum(f.freight_value)             as freight,
        max(f.is_late)                   as is_late
    from {{ ref('fact_orders') }} f
    join {{ ref('dim_customer') }} c on c.customer_key = f.customer_key
    where f.order_status <> 'canceled'
    group by f.order_id, c.customer_unique_id
),

first_purchase as (
    select customer_unique_id, date_trunc('month', min(purchased_at)) as first_month
    from orders
    group by customer_unique_id
)

select
    date_trunc('month', o.purchased_at)::date                                   as month,
    count(*)                                                                    as orders,
    count(distinct o.customer_unique_id)                                        as customers,
    count(distinct o.customer_unique_id)
        filter (where fp.first_month = date_trunc('month', o.purchased_at))     as new_customers,
    count(distinct o.customer_unique_id)
        filter (where fp.first_month < date_trunc('month', o.purchased_at))     as returning_customers,
    sum(o.items)                                                                as items_sold,
    round(sum(o.revenue)::numeric, 2)                                           as revenue,
    round(sum(o.freight)::numeric, 2)                                           as freight,
    round((sum(o.revenue) / count(*))::numeric, 2)                              as avg_order_value,
    round(100.0 * avg(o.is_late), 1)                                            as late_delivery_pct
from orders o
join first_purchase fp on fp.customer_unique_id = o.customer_unique_id
group by 1
