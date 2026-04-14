{{ config(
    materialized='table',
    post_hook=[
      "optimize {{ this }} zorder by (service_category)"
    ]
) }}

select
    dim.service_category,
    count(*) as total_logs,

    -- audit columns
    max(fct._occurred_at) as _data_up_to_at,
    {{ audit_columns() }}

from {{ ref('fct_microservices_logs') }} as fct
inner join {{ ref('dim_services') }} as dim
    on
        fct.service_key = dim.service_key
        and fct.is_corrupted = false
        and dim.is_current = true
group by dim.service_category
