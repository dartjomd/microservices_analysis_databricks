{% macro drop_ci_schema(schema_name) %}
    
    {% set drop_query %}
        DROP SCHEMA IF EXISTS {{ target.catalog }}.{{ schema_name }} CASCADE
    {% endset %}

    {% do run_query(drop_query) %}
    {% do log("Cleanup: Dropped schema " ~ target.catalog ~ "." ~ schema_name, info=True) %}

{% endmacro %}