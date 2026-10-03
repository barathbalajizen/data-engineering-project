{#
  WHERE clause for an incremental fact: rows whose order changed since the last load, plus rows whose key is
  not in the table yet (late-arriving records, e.g. restored by a backfill with an OLD updated_at, which a
  plain "updated_at >= max" filter would miss forever).

  If the target table does not have `updated_col` yet (it was built before it became incremental), no filter
  is applied: everything is reprocessed once with delete+insert and on_schema_change adds the column.
#}
{% macro incremental_predicate(updated_expr, updated_col, key_expr, key_col) %}
  {%- if is_incremental() -%}
    {%- set existing = adapter.get_columns_in_relation(this) | map(attribute='name') | list -%}
    {%- if updated_col in existing -%}
    where {{ updated_expr }} >= (select coalesce(max({{ updated_col }}), timestamp '1900-01-01') from {{ this }})
       or not exists (select 1 from {{ this }} existing_rows where existing_rows.{{ key_col }} = {{ key_expr }})
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
