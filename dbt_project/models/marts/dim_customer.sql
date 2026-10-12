-- DIMENSION: customer, with history (SCD Type 2)
-- What:  One row per VERSION of a customer. When a customer's city or state changes, the old row is closed
--        and a new row is added, so the history is kept instead of overwritten.
-- Source: the snapshot customers_snapshot (snapshots/customers_snapshot.sql) records the changes; this model
--        only renames its columns.
--
-- Example: customer C1 moves from sao paulo to rio de janeiro on 2018-06-01
--   customer_key | customer_id | customer_city   | valid_from | valid_to   | is_current
--   k1           | C1          | sao paulo       | 2017-01-10 | 2018-06-01 | false
--   k2           | C1          | rio de janeiro  | 2018-06-01 | 9999-12-31 | true
-- An order placed in 2017 points to k1, so reports show the city at the time of the order.
--
-- Columns:
--   customer_key  unique id of one version (one customer_id can have several). The facts store this key.
--   valid_from    when this version became true
--   valid_to      when it stopped being true. 9999-12-31 means "still current": a far-future date instead
--                 of NULL keeps the "valid at time X" comparison simple (valid_from <= X < valid_to)
--   is_current    true for the latest version of each customer
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
