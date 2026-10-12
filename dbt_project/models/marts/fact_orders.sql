-- FACT: order items (the main sales table)
-- Grain: one row per item sold. An order with 3 products = 3 rows.
-- Each row holds keys to the dimensions (customer, product, seller, date) and the numbers to sum or average
-- (price, freight, delivery days, late flag).
--
-- Steps:
--   1. Take every order item with its order's details (int_order_items_enriched).
--   2. Build the keys: order_item_key = hash of order_id + item number (one per row); product_key and
--      seller_key = the same hashes the dimensions use; date_key = purchase date as YYYYMMDD.
--   3. Attach the customer VERSION that was valid when the order was placed (see dim_customer). If a
--      customer moved in June, their March orders still point to the March version: history is not rewritten.
--   4. Incremental load: the first run loads everything. Later runs rebuild only rows of new or changed
--      orders (macro incremental_filter, in macros/incremental_predicate.sql). dbt deletes those keys from
--      the table and inserts the new rows (delete+insert), so a changed order replaces its old rows instead
--      of being added twice.
--
-- Config: on_schema_change adds new columns to the existing table. The post_hook creates a unique index on
--         order_item_key, so Postgres itself rejects a duplicate row.
{{ config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    post_hook=[
        "{{ fact_index(this, 'order_item_key', unique=True) }}",
    ]
) }}

with candidates as (
    select
        md5(e.order_id || '-' || e.order_item_id::text)              as order_item_key,
        e.order_id,
        e.order_item_id,
        c.customer_key,
        md5(e.product_id)                                            as product_key,
        md5(e.seller_id)                                             as seller_key,
        to_char(e.order_purchase_timestamp, 'YYYYMMDD')::int         as date_key,
        e.order_status,
        e.order_purchase_timestamp,
        e.price,
        e.freight_value,
        e.delivery_days,
        e.is_late,
        e.order_updated_at
    from {{ ref('int_order_items_enriched') }} e
    -- the customer version that was valid at purchase time: valid_from <= purchase < valid_to
    left join {{ ref('dim_customer') }} c
        on  c.customer_id = e.customer_id
        and e.order_purchase_timestamp >= c.valid_from
        and e.order_purchase_timestamp <  c.valid_to
)

select candidates.*
from candidates
{{ incremental_filter('candidates', 'order_updated_at', 'order_item_key') }}
