{{ config(
    materialized='incremental',
    unique_key='query_id'
) }}

with raw_history as (
    select * from system.query.history
)

select
    rh.statement_id as query_id,
    rh.start_time,
    rh.executed_by as user_name,
    rh.statement_text as query_text,
    rh.execution_status,
    rh.compute.warehouse_id, -- noqa: RF01
    rh.total_duration_ms / 1000 as duration_seconds,
    rh.total_task_duration_ms / 1000 as compute_seconds,
    regexp_extract(
        rh.statement_text,
        'invocation_id: ([a-z0-9-]+)',
        1
    ) as dbt_invocation_id
from raw_history as rh
where
    rh.executed_by = 'e97e7013-64c4-41c4-92af-b2269ef83ee2'

    {% if is_incremental() %}
        and rh.start_time > (select max(t.start_time) from {{ this }} as t)
    {% endif %}
