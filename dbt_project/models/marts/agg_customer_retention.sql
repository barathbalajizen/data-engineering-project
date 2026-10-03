-- Monthly cohort retention. Cohort = month of a customer's first (non-canceled) order; for every later
-- month, the share of the cohort that ordered again. Month 0 is always 100%.
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
