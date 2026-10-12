{#
  MACRO incremental_filter: decides which rows an incremental fact rebuilds on this run.

  Used at the end of a fact model:
      select candidates.* from candidates {{ incremental_filter('candidates', 'order_updated_at', 'order_item_key') }}

  A row is rebuilt when EITHER
    1. its order changed since the last run: its updated_at >= the newest updated_at already in the table
       (">=" rather than ">": rows at exactly that time are rebuilt again; harmless with delete+insert,
       and nothing can be missed), OR
    2. its key is not in the table yet (a late-arriving row).

  Why rule 2 is needed: a backfill can restore old rows whose updated_at is OLDER than the newest one in the
  table. Rule 1 alone would skip them forever. Example: the newest updated_at in fact_orders is 2018-09-01,
  and a backfill restores an order last updated on 2017-03-10. Rule 1 says no; rule 2 says yes, because its
  key is missing from the table.

  How rule 2 is written: LEFT JOIN the existing table and keep the rows that found no match
  (existing_rows.key IS NULL). Postgres runs this as one fast hash join. An earlier version with
  "OR NOT EXISTS" did not finish in 17 minutes; see docs/performance.md.

  When there is no filter:
    - first run (the table does not exist yet, is_incremental() is false): everything is loaded
    - the table exists but does not have the updated_at column yet: everything is rebuilt once, and
      on_schema_change adds the column
#}
{% macro incremental_filter(alias, updated_col, key_col) %}
  {%- if is_incremental() -%}
    {%- set existing = adapter.get_columns_in_relation(this) | map(attribute='name') | list -%}
    {%- if updated_col in existing -%}
    left join {{ this }} existing_rows
        on existing_rows.{{ key_col }} = {{ alias }}.{{ key_col }}
    where {{ alias }}.{{ updated_col }} >= (
              select coalesce(max({{ updated_col }}), timestamp '1900-01-01') from {{ this }})
       or existing_rows.{{ key_col }} is null
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
