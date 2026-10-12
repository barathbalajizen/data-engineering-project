-- AGGREGATE: product sales
-- What:  One row per product: orders, units sold, revenue, average price, first and last sale date,
--        revenue_rank (1 = highest revenue) and revenue_share_pct (% of all revenue). Non-canceled orders only.
select
    p.product_key,
    p.product_id,
    p.category,
    count(distinct f.order_id)                                          as orders,
    count(*)                                                            as units_sold,
    round(sum(f.price)::numeric, 2)                                     as revenue,
    round(avg(f.price)::numeric, 2)                                     as avg_price,
    min(f.order_purchase_timestamp)::date                               as first_sale,
    max(f.order_purchase_timestamp)::date                               as last_sale,
    rank() over (order by sum(f.price) desc)                            as revenue_rank,
    round((100.0 * sum(f.price) / sum(sum(f.price)) over ())::numeric, 3)  as revenue_share_pct
from {{ ref('fact_orders') }} f
join {{ ref('dim_product') }} p on p.product_key = f.product_key
where f.order_status <> 'canceled'
group by p.product_key, p.product_id, p.category
