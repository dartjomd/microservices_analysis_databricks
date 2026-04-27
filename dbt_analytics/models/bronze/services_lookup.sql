{% set raw_schema = get_raw_schema('bronze') %}

{{ config(
    pre_hook=[
        "CREATE SCHEMA IF NOT EXISTS " ~ raw_schema,

        "CREATE TABLE IF NOT EXISTS " ~ raw_schema ~ ".services_lookup_raw (
            service_name string,
            service_category string,
            criticality_tier string,
            _file_source_name string,
            _ingested_at timestamp
        ) USING DELTA",
        
        "COPY INTO " ~ raw_schema ~ ".services_lookup_raw
         FROM (
             SELECT 
                 service_name,
                 service_category,
                 criticality_tier,
                 _metadata.file_path AS _file_source_name,
                 current_timestamp() AS _ingested_at
             FROM 's3://de-practice-artjom-s3/services'
         )
         FILEFORMAT = CSV
         FORMAT_OPTIONS (
             'header' = 'true',
             'inferSchema' = 'false',
             'mergeSchema' = 'true'
         )
         COPY_OPTIONS ('mergeSchema' = 'true')"
    ]
) }}

SELECT * FROM {{ raw_schema }}.services_lookup_raw