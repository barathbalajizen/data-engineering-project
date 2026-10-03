-- One row per order (latest valid version from Silver). Light renaming only; business logic lives downstream.
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
