{% macro audit_columns() %}
    current_timestamp() as _dbt_updated_at,
    '{{ invocation_id }}' as _dbt_invocation_id
{% endmacro %}