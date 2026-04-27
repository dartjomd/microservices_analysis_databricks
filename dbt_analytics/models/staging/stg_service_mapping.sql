select
    lower(trim(cast(service_name as string))) as service_name,
    service_category,
    criticality_tier,

    -- audit columns
    {{ audit_columns() }}
from {{ ref('services_lookup') }}
