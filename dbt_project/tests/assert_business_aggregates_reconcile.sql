-- The business aggregates must agree with each other and with the daily aggregate: same revenue and order
-- count by month, by product and by day; and every retention cohort starts at 100% in its first month.
with daily as (select coalesce(sum(revenue), 0) as revenue, coalesce(sum(orders), 0) as orders
               from {{ ref('agg_daily_sales') }}),
monthly as (select coalesce(sum(revenue), 0) as revenue, coalesce(sum(orders), 0) as orders
            from {{ ref('agg_monthly_kpis') }}),
products as (select coalesce(sum(revenue), 0) as revenue from {{ ref('agg_product_sales') }}),
bad_cohorts as (select count(*) as n from {{ ref('agg_customer_retention') }}
                where months_since_first = 0 and retention_pct <> 100)
select daily.revenue as daily_revenue, monthly.revenue as monthly_revenue, products.revenue as product_revenue,
       daily.orders as daily_orders, monthly.orders as monthly_orders, bad_cohorts.n as cohorts_not_100
from daily, monthly, products, bad_cohorts
where abs(daily.revenue - monthly.revenue) > 0.05
   or abs(daily.revenue - products.revenue) > 0.05
   or daily.orders <> monthly.orders
   or bad_cohorts.n > 0
