{% test non_negative(model, column_name) %}
-- Fails on rows where the column is below zero (NULL passes; combine with not_null to forbid it)
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
