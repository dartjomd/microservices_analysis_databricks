{% macro get_raw_schema(base_schema) %}
    {# if in CI and not pushing in main use fake schema #}
    {%- if target.name == 'ci' and env_var('GITHUB_REF_NAME', '') != 'main' -%}
        {{ "ci_schema_" ~ env_var('GITHUB_RUN_ID', 'local') }}
    {%- else -%}
        {# if production/development or push into main in CI add _raw to base schema #}
        {{ base_schema ~ '_raw' }}
    {%- endif -%}
{% endmacro %}