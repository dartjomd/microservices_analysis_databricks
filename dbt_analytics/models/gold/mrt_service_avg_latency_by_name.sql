{{ config(
    materialized='table',
    post_hook=[
      "optimize {{ this }} zorder by (service_name)"
    ]
) }}

select
    service_name,
    avg(latency_ms) as avg_latency_ms,
    count(*) as total_requests,

    -- audit columns
    max(_occurred_at) as _data_up_to_at,
    {{ audit_columns() }}
from {{ ref('fct_microservices_logs') }}
group by service_name
