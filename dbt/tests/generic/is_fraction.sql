-- Custom generic test: a "share" column must behave like a fraction —
-- between 0 and 1 inclusive (or NULL where genuinely unknown).
-- dbt_utils would give us this, but the package is deliberately not used
-- so CI needs no `dbt deps` / network access: one small local test instead.
-- Rows returned by this query = failures.
{% test is_fraction(model, column_name) %}

select
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 1)

{% endtest %}
