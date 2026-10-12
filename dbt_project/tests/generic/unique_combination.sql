{#
  GENERIC TEST unique_combination: a primary key made of several columns must be unique.
  In schema.yml:  data_tests: [{unique_combination: {columns: [order_id, order_item_id]}}]
  Returns every combination that appears more than once.
#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
