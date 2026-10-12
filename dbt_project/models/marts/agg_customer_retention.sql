-- AGGREGATE: monthly cohort retention ("of the people who first bought in month X, how many came back?")
-- Cohort:  all customers whose FIRST non-canceled order was in the same month.
-- For each cohort and each later month: how many of those customers ordered again, as a % of the cohort.
--
-- Example rows (made-up numbers):
--   cohort_month | months_since_first | cohort_size | active_customers | retention_pct
--   2017-01-01   | 0                  | 800         | 800              | 100.0
--   2017-01-01   | 1                  | 800         | 20               | 2.5
--   2017-01-01   | 2                  | 800         | 16               | 2.0
-- Read as: 800 people first ordered in January 2017; 20 of them ordered again in February (1 month later),
-- 16 in March. Month 0 is always 100% (everyone in the cohort ordered that month, by definition).
--
-- Steps: order_months (who ordered in which month) -> cohorts (each customer's first month) ->
--        cohort_sizes -> activity (customers active per cohort and month offset) -> percentage.
with order_months as (
    select distinct
        c.customer_unique_id,
        date_trunc('month', f.order_purchase_timestamp)::date  as order_month
    from {{ ref('fact_orders') }} f
    join {{ ref('dim_customer') }} c on c.customer_key = f.customer_key
    where f.order_status <> 'canceled'
),

cohorts as (
    select customer_unique_id, min(order_month) as cohort_month
    from order_months
    group by customer_unique_id
),

cohort_sizes as (
    select cohort_month, count(*) as cohort_size
    from cohorts
    group by cohort_month
),

activity as (
    select
        c.cohort_month,
        -- months between the cohort month and this month, e.g. Jan 2017 -> Mar 2017 = 2
        ((extract(year from m.order_month) - extract(year from c.cohort_month)) * 12
          + extract(month from m.order_month) - extract(month from c.cohort_month))::int  as months_since_first,
        count(distinct m.customer_unique_id)                                              as active_customers
    from order_months m
    join cohorts c on c.customer_unique_id = m.customer_unique_id
    group by 1, 2
)

select
    a.cohort_month,
    a.months_since_first,
    s.cohort_size,
    a.active_customers,
    round(100.0 * a.active_customers / s.cohort_size, 1)   as retention_pct
from activity a
join cohort_sizes s on s.cohort_month = a.cohort_month
