{{
  config(
    materialized = 'incremental',
    unique_key = 'log_id'
    )
}}

with incremented_stg_data as (
    select
        log_id,
        service_name,
        latency_ms,
        status_code,
        _occurred_at,
        _dbt_updated_at as _stg_updated_at,
        _source_file_path
    from {{ ref('stg_microservices_logs') }}
    {% if is_incremental() %}

        where
            _occurred_at
            > (
                select date_add(max(_date._occurred_at), -3)
                from {{ this }} as _date
            )

    {% endif %}
),

check_service_existance as (
    select
        d.log_id,
        d.latency_ms,
        d.status_code,
        d._occurred_at,
        d._stg_updated_at,
        d._source_file_path,

        case
            when s.service_name is null then "other"
            else d.service_name
        end as service_name

    from incremented_stg_data as d
    left join
        {{ ref('stg_service_mapping') }} as s
        on d.service_name = s.service_name

),

add_md5 as (
    select
        *,
        md5(service_name) as service_key
    from check_service_existance
),

dim_joined_data as (
    select
        incr_stg.*,
        dim.service_category,
        dim.criticality_tier
    from add_md5 as incr_stg
    left join {{ ref('dim_services') }} as dim
        on
            incr_stg.service_key = dim.service_key
            and dim.is_current = true
),

validated_data as (
    select
        *,

        -- is_slow flag
        coalesce(latency_ms > 500, false) as is_slow,

        -- is_corrupted flag
        coalesce(
            status_code is null
            or
            latency_ms is null
            or
            trim(lower(service_category)) = "unknown", false
        ) as is_corrupted,

        -- audit columns
        {{ audit_columns() }}

    from dim_joined_data
)

select
    *,

    case
        when is_corrupted then "Data Error"
        when status_code between 200 and 299 then "Success"
        when status_code between 400 and 499 then "Client Error"
        when status_code between 500 and 599 then "Server Error"
        else "Unknown"
    end as status_category

from validated_data
{{ is_run_limited() }}
