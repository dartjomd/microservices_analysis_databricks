{% set raw_schema = get_raw_schema('bronze') %}

{{ config(
    materialized='view',
    schema='bronze'
) }}

-- Display data via view
select * from {{ source('s3_bronze', 'services_lookup_raw') }}
{{ is_run_limited() }}
