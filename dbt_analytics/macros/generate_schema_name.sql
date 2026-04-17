{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {# 
       create ci_schema_{run_id} only if target is CI and we dont push into main:
       otherwise manifest will have ci_schema_{run_id} instead of silver
    #}
    {%- if target.name == 'ci' and env_var('GITHUB_REF_NAME', '') != 'main' -%}
        
        {{ "ci_schema_" ~ env_var('GITHUB_RUN_ID', 'local') }}

    {%- else -%}
        
        {{ custom_schema_name | default(default_schema, true) }}
        
    {%- endif -%}

{%- endmacro %}