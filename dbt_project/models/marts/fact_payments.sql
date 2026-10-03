-- Grain: one row per order payment. Incremental like fact_orders (payments are reprocessed with their order).
{{ config(
    materialized='incremental',
    unique_key='payment_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    post_hook=[
        "{{ fact_index(this, 'payment_key', unique=True) }}",
    ]
) }}

with candidates as (
    select
        md5(p.order_id || '-' || p.payment_sequential::text)     as payment_key,
        p.order_id,
        p.payment_sequential,
        p.payment_type,
        p.payment_value,
        to_char(p.order_purchase_timestamp, 'YYYYMMDD')::int     as date_key,
        p.order_updated_at
    from {{ ref('int_order_payments') }} p
)

select candidates.*
from candidates
{{ incremental_filter('candidates', 'order_updated_at', 'payment_key') }}
