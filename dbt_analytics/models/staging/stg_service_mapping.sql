
select
    lower(trim(cast(service_name as string))) as service_name,
    service_category,
    criticality_tier,

    -- audit columns
    {{audit_columns()}}
from {{ source('s3_bronze', 'services_lookup') }} 