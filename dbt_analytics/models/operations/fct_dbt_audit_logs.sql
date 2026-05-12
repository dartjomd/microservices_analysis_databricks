{{ config(
    materialized='incremental',
    unique_key='query_id'
) }}

with raw_history as (
    select * from system.query.history
)

select
    statement_id as query_id,
    start_time,
    executed_by as user_name,
    statement_text as query_text,
    execution_status,
    total_duration_ms / 1000 as duration_seconds,
    total_task_duration_ms / 1000 as compute_seconds,
    compute.warehouse_id as warehouse_id,
    regexp_extract(statement_text, 'invocation_id: ([a-z0-9-]+)', 1) as dbt_invocation_id
from raw_history
where executed_by = 'e97e7013-64c4-41c4-92af-b2269ef83ee2' -- service principar application ID

{% if is_incremental() %}
  and start_time > (select max(start_time) from {{ this }})
{% endif %}
