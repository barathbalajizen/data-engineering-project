{#
  Incremental filter for a fact built as `select ... from candidates c {{ incremental_filter('c', ...) }}`:
  keeps rows whose order changed since the last load, plus rows whose key is not in the table yet
  (late-arriving records, e.g. restored by a backfill with an OLD updated_at, which a plain
  "updated_at >= max" filter would miss forever).

  Written as LEFT JOIN ... IS NULL (an anti-join Postgres runs as one hash join). The first version used
  `updated_at >= max OR NOT EXISTS (...)`: because of the OR, Postgres cannot turn NOT EXISTS into an
  anti-join and runs it as a correlated subquery for every row. Measured on 250k rows without an index it did
  not finish in 17 minutes (planner cost 3.5e9, vs about 9 s for a full refresh); see docs/performance.md.

  If the target table does not have `updated_col` yet (built before it became incremental), no filter is
  applied: everything is reprocessed once with delete+insert and on_schema_change adds the column.
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
