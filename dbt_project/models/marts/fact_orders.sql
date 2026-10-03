-- Grain: one row per order item. Customer is joined to the dimension version that was valid at purchase
-- time (point-in-time SCD2 join), so a later move does not rewrite history.
-- Incremental: only items whose order changed (or that are new) are rebuilt, with delete+insert on the key.
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
    left join {{ ref('dim_customer') }} c
        on  c.customer_id = e.customer_id
        and e.order_purchase_timestamp >= c.valid_from
        and e.order_purchase_timestamp <  c.valid_to
)

select candidates.*
from candidates
{{ incremental_filter('candidates', 'order_updated_at', 'order_item_key') }}
