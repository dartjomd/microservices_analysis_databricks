{% macro cleanup_old_ci_schemas(catalog='s3_learn', prefix='ci_schema_') %}
    
    {% set get_schemas_query %}
        SHOW SCHEMAS IN {{ catalog }}
    {% endset %}

    {% set results = run_query(get_schemas_query) %}

    {% if execute %}
        {% for row in results %}
            {% set schema_name = row[0] %}
            {% if schema_name.startswith(prefix) %}
                {% do log("Dropping schema: " ~ catalog ~ "." ~ schema_name, info=True) %}
                {% do run_query("DROP SCHEMA IF EXISTS " ~ catalog ~ "." ~ schema_name ~ " CASCADE") %}
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}