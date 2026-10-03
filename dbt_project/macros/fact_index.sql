{#
  CREATE INDEX IF NOT EXISTS for an incremental fact, used as a post-hook. dbt's `indexes` config is only
  applied when a table is created, so an existing incremental table would never get it; a post-hook runs
  on every build and is a no-op once the index exists.
  Only the unique key is indexed, for integrity: Postgres itself rejects a duplicate key (the dbt unique test
  only finds it afterwards). Measured on 250k rows (docs/performance.md): with the anti-join incremental filter
  indexes give no speed-up (3.2 s vs 3.3 s for a 1% change) and cost about 12% on a full refresh, so no
  index is added for speed (an order_updated_at index was tried and removed).
  var fact_indexes=false skips it (the benchmark measures both).
#}
{% macro fact_index(relation, column, unique=False) %}
  {%- if var('fact_indexes', true) -%}
    create {{ 'unique' if unique }} index if not exists {{ relation.identifier }}__{{ column }}
        on {{ relation }} ({{ column }})
  {%- else -%}
    select 1
  {%- endif -%}
{% endmacro %}
