{% snapshot snap_dim_services %}

{{
    config(
      target_schema='silver',
      unique_key='service_name',
      strategy='check',
      check_cols=['criticality_tier', 'service_name'],
      tags=['silver', 'historical']
    )
}}

select * from {{ source('s3_bronze', 'services_lookup') }}

{% endsnapshot %}