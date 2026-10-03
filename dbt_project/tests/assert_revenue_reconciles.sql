-- Business metric reconciliation inside Gold: revenue and order counts of the daily aggregate must equal the
-- order items they come from (non-canceled orders). Catches dropped rows, e.g. dates outside dim_date.
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
