-- TEST: daily revenue reconciles with the order items
-- Passes when it returns no rows. Total revenue and order count in agg_daily_sales must equal the order
-- items they come from (non-canceled orders). Catches lost rows, e.g. an order date missing from dim_date.
with items as (
    select coalesce(sum(price), 0) as revenue, count(distinct order_id) as orders
    from {{ ref('int_order_items_enriched') }}
    where order_status <> 'canceled'
),
agg as (
    select coalesce(sum(revenue), 0) as revenue, coalesce(sum(orders), 0) as orders
    from {{ ref('agg_daily_sales') }}
)
select items.revenue as items_revenue, agg.revenue as agg_revenue, items.orders as items_orders,
       agg.orders as agg_orders
from items, agg
where abs(items.revenue - agg.revenue) > 0.01 or items.orders <> agg.orders
