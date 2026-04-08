{{ config(
    materialized='table',
    post_hook=[
      "optimize {{ this }} zorder by (report_trunc_time)"
    ]
) }}

with joined_data as (
    select
        fct.status_category,
        fct.latency_ms,
        fct._occurred_at,
        date_trunc('MINUTE', fct._occurred_at) as report_trunc_time
    from {{ ref('fct_microservices_logs') }} as fct
    inner join {{ ref('dim_services') }} as dim
        on fct.service_key = dim.service_key
        and dim.is_current = true
    where fct.is_corrupted = false
),

final_metrics as (
    select
        report_trunc_time,
        avg(latency_ms) as avg_latency,
        max(latency_ms) as max_latency,
        count(case when status_category like '%Error%' then 1 end) as error_count,
        count(*) as total_count,
        {{ calculate_rate('status_category', 'Success') }} as success_rate,
        
        -- audit columns
        max(_occurred_at) as _data_up_to_at,
        {{ audit_columns() }}
        
    from joined_data
    group by report_trunc_time
)

select * from final_metrics