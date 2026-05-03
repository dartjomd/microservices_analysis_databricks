{% set raw_schema = get_raw_schema('bronze') %}

-- Create schema for pre hook raw data
-- Create table for pre hook data
-- Fill pre hook table with data from S3
{{ config(
    schema='bronze',
    pre_hook=[
        "create schema if not exists " ~ raw_schema,

        "create table if not exists " ~ raw_schema ~ ".services_lookup_raw (
            service_name string,
            service_category string,
            criticality_tier string,
            _file_source_name string,
            _ingested_at timestamp
        ) using delta",
        
        "copy into " ~ raw_schema ~ ".services_lookup_raw
         from (
             select 
                 service_name,
                 service_category,
                 criticality_tier,
                 _metadata.file_path as _file_source_name,
                 current_timestamp() as _ingested_at
             from 's3://de-practice-artjom-s3/services'
         )
         fileformat = csv
         format_options (
             'header' = 'true',
             'inferschema' = 'false',
             'mergeschema' = 'true'
         )
         copy_options (
             'mergeSchema' = 'true'
             {% if target.name == 'ci' %}
             , 'maxFiles' = '5'
             {% endif %}
         )"
    ]
) }}

-- Display data via view
select * from {{ raw_schema }}.services_lookup_raw
