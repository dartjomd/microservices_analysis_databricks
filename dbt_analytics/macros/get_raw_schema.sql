{% macro get_raw_schema(base_schema) %}
    {{ base_schema ~ '_raw' }}
{% endmacro %}