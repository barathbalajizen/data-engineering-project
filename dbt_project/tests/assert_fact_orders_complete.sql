-- TEST: the incremental fact_orders is complete and up to date
-- How dbt tests work: a test PASSES when its query returns no rows. Every row returned is a problem.
-- This test compares fact_orders with what a full rebuild would produce (int_order_items_enriched) and
-- returns every difference:
--   missing_or_changed   should be in the fact but is missing or has old values
--                        (e.g. a late-arriving row that the incremental filter skipped)
--   unexpected_or_stale  is in the fact but should not be (anymore)
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
