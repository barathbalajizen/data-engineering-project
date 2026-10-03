-- The incremental fact must contain exactly the current order items: nothing missing (e.g. a late-arriving
-- record skipped by the incremental filter) and nothing stale. Returns the differing keys.
with expected as (
    select md5(order_id || '-' || order_item_id::text) as order_item_key, order_status, price
    from {{ ref('int_order_items_enriched') }}
),
actual as (
    select order_item_key, order_status, price from {{ ref('fact_orders') }}
)
(select 'missing_or_changed' as problem, * from (select * from expected except select * from actual) m)
union all
(select 'unexpected_or_stale' as problem, * from (select * from actual except select * from expected) s)
