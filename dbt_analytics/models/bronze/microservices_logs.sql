{% set raw_schema = get_raw_schema('bronze') %}

{{ config(
    materialized='view',
    schema='bronze',
    pre_hook=[
        "CREATE SCHEMA IF NOT EXISTS " ~ raw_schema,

        "CREATE TABLE IF NOT EXISTS " ~ raw_schema ~ ".microservices_logs_raw (
            latency_ms bigint,
            service_name string,
            status_code string,
            timestamp string,
            _file_source_name string,
            _ingested_at timestamp
        ) USING DELTA",
        
        "COPY INTO " ~ raw_schema ~ ".microservices_logs_raw
         FROM (
             SELECT 
                 latency_ms,
                 service_name,
                 status_code,
                 timestamp,
                 _metadata.file_path AS _file_source_name,
                 current_timestamp() AS _ingested_at
             FROM 's3://de-practice-artjom-s3/landing'
         )
         FILEFORMAT = JSON
         COPY_OPTIONS ('mergeSchema' = 'true')"
    ]
) }}

SELECT * FROM {{ raw_schema }}.microservices_logs_raw