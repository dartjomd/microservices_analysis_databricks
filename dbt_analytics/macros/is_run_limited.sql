{% macro is_run_limited() %}
    -- If in CI mode limit query to only 1000 rows to avoid extra work
    {% if target.name in ('ci') %}
        limit 1000
    {% endif %}
{% endmacro %}