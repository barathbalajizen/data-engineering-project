-- STAGING: customers
-- What:  One row per customer_id.
-- Note:  In this dataset (Olist) every ORDER gets its own customer_id. The real person is customer_unique_id,
--        so "how many customers" questions count customer_unique_id.
select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    updated_at
from {{ source('staging', 'customers') }}
