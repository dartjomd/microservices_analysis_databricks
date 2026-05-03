with raw_logs as (
    select * from {{ ref('microservices_logs') }}
)

select

    -- log id
    md5(
        concat_ws(
            '|',
            coalesce(
                nullif(lower(trim(cast(service_name as string))), ''),
                'unknown'
            ),
            coalesce(cast(status_code as string), '0'),
            coalesce(cast(timestamp as string), '1970-01-01')
        )
    ) as log_id,

    -- status code
    case
        when
            try_cast(status_code as int) is null
            or try_cast(status_code as int) not in (
                200, 201, 204, 400, 401, 403, 404, 409, 500, 502, 503, 504
            )
            then null
        else try_cast(status_code as int)
    end as status_code,

    -- service name
    coalesce(
        nullif(lower(trim(cast(service_name as string))), ''),
        'other'
    ) as service_name,

    -- latency 
    case
        when try_cast(latency_ms as int) < 0 then null
        else try_cast(latency_ms as int)
    end as latency_ms,

    try_cast(timestamp as timestamp) as _occurred_at,
    _file_source_name as _source_file_path,

    -- audit columns
    {{ audit_columns() }}
from raw_logs
{{ is_run_limited() }}
