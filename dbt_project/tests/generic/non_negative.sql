{#
  GENERIC TEST non_negative: use it in schema.yml as `data_tests: [non_negative]`.
  Returns (= fails on) the rows where the column is below zero. NULL passes; add not_null to forbid it.
#}
{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
