-- STAGING: orders
-- What:  One row per order, read from staging.orders (loaded from Silver by Spark, src/load_warehouse.py).
-- Why:   Staging models are a thin layer between the loaded tables and the business logic. They only pick
--        and rename columns. Every later model reads these, never the loaded tables directly, so a renamed
--        source column has to be fixed in one place only.
-- Note:  Silver already keeps only the latest valid version of each order, so there are no duplicates here.
--        updated_at = when the order last changed in the source; the incremental facts use it.
select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    updated_at,
    ingestion_ts,
    batch_id
from {{ source('staging', 'orders') }}
