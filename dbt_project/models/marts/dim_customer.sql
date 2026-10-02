-- SCD Type 2: one row per customer version
select
    dbt_scd_id                                   as customer_key,
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    dbt_valid_from                               as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as valid_to,
    (dbt_valid_to is null)                       as is_current
from {{ ref('customers_snapshot') }}
