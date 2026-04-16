{% macro generate_schema_name(custom_schema_name, node) -%}

    {# don't apply macro if in CI mode #}
    {%- if target.name == 'ci' -%}
        {{ "ci_schema_" ~ env_var('GITHUB_RUN_ID', 'local') }}
    {%- else -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}