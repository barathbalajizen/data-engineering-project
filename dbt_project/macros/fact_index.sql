{#
  MACRO fact_index: creates an index on a fact table if it does not exist yet. Used as a post_hook, so it runs
  after every build (and does nothing once the index exists). dbt's own `indexes` setting only applies when a
  table is first created, which an incremental table rarely is.

  The facts get only a UNIQUE index on their key, so Postgres itself rejects a duplicate row. Indexes for speed
  were measured and gave no gain (docs/performance.md, section "PostgreSQL indexes and queries").
  dbt var fact_indexes=false skips it (used by the benchmark).
#}
{% macro fact_index(relation, column, unique=False) %}
  {%- if var('fact_indexes', true) -%}
    create {{ 'unique' if unique }} index if not exists {{ relation.identifier }}__{{ column }}
        on {{ relation }} ({{ column }})
  {%- else -%}
    select 1
  {%- endif -%}
{% endmacro %}
