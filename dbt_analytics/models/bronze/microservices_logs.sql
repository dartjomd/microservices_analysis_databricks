{% set raw_schema = get_raw_schema('bronze') %}

-- Create schema for pre hook raw data
-- Create physical table
-- Load data into pre hook table from S3
{{ config(
    schema='bronze',
    pre_hook=[
        "create schema if not exists " ~ raw_schema,

        "create table if not exists " ~ raw_schema ~ ".microservices_logs_raw (
            latency_ms bigint,
            service_name string,
            status_code string,
            timestamp string,
            _file_source_name string,
            _ingested_at timestamp
        ) using delta",
        
        "copy into " ~ raw_schema ~ ".microservices_logs_raw
         from (
             select 
                 latency_ms,
                 service_name,
                 status_code,
                 timestamp,
                 _metadata.file_path as _file_source_name,
                 current_timestamp() as _ingested_at
             from 's3://de-practice-artjom-s3/landing'
         )
         fileformat = json
         copy_options ('mergeSchema' = 'true')"
    ]
) }}

-- Display data via view
select * from {{ raw_schema }}.microservices_logs_raw
