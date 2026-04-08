{% macro calculate_rate(column, expected_value, precision=2) %}
  

    round(count(case when {{ column }} like '%{{ expected_value }}%' then 1 end) * 100.0 / nullif(count(*), 0), {{precision}})


{% endmacro %}