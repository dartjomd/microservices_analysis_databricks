with source as (
        select * from {{ source('s3_bronze', 'microservices_logs_raw') }}
  ),
  renamed as (
      select
          {{ adapter.quote("latency_ms") }},
        {{ adapter.quote("service_name") }},
        {{ adapter.quote("status_code") }},
        {{ adapter.quote("timestamp") }},
        {{ adapter.quote("_file_source_name") }},
        {{ adapter.quote("_ingested_at") }}

      from source
  )
  select * from renamed
    