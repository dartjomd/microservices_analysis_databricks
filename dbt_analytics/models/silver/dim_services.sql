with source_data as (
    select * from {{ ref('snap_dim_services') }}
),

processed as (
    select
        -- name and surrogate key
        lower(trim(cast(service_name as string))) as service_name,
        md5(lower(trim(cast(service_name as string)))) as service_key,

        -- attributes
        coalesce(service_category, 'unknown') as service_category,
        coalesce(criticality_tier, 'unknown') as criticality_tier,

        -- SCD2 metadata
        dbt_valid_from as effective_from,
        dbt_valid_to as effective_to,
        (dbt_valid_to is null) as is_current,

        -- audit columns
        {{ audit_columns() }}

    from source_data
),

placeholder as (
    select
        'other' as service_name,
        md5('other') as service_key,
        'unknown' as service_category,
        'low' as criticality_tier,
        cast('1970-01-01' as timestamp) as effective_from,
        null as effective_to,
        true as is_current,

        -- audit columns
        {{ audit_columns() }}
)

select * from processed
union all
select * from placeholder
