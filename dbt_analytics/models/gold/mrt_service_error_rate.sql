{{ config(
    materialized='table',
    post_hook=[
      "optimize {{ this }} zorder by (report_trunc_time)"
    ]
) }}

select
    report_trunc_time,
    sum(error_count) as total_errors,
    sum(total_count) as total_records,

    -- audit columns
    max(max(_data_up_to_at)) over () as _data_up_to_at,
    {{ audit_columns() }}
from {{ ref('mrt_service_health_hourly') }}
group by report_trunc_time
