{% macro generate_schema_name(custom_schema_name, node) -%}

    -- don't apply macro if in CI mode
    {% if target.name == 'ci' %}
        {{ target.schema }}
    {%- else -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}