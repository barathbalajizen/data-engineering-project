-- AGGREGATE: monthly KPIs
-- What:  One row per month: orders, customers (new vs returning), items, revenue, freight, average order
--        value and the share of late deliveries.
-- Definitions:
--   customer             a person (customer_unique_id), not customer_id (which is new for every order)
--   new customer         their first order ever was in this month
--   returning customer   they had ordered in an earlier month
--   revenue              item prices of non-canceled orders (freight separate), as in agg_daily_sales
--   avg_order_value      revenue / number of orders
--   late_delivery_pct    % of orders delivered after the estimated date
-- Steps: (1) collapse items into one row per order, (2) find each customer's first month,
--        (3) group by month.
-- (1) one row per order (fact_orders has one row per item)
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

-- (2) the month of each customer's first order: decides new vs returning
first_purchase as (
    select customer_unique_id, date_trunc('month', min(purchased_at)) as first_month
    from orders
    group by customer_unique_id
)

-- (3) one row per month
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
