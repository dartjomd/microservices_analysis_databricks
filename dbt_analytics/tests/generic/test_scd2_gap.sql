{% test scd2_gap(model, unique_key, effective_to, effective_from) %}

with services_versions as (
    select
        {{unique_key}},
        {{effective_to}} as effective_to,
        {{effective_from}} as effective_from,
        lead({{effective_from}})
            over (
                partition by {{unique_key}}
                order by {{effective_from}} asc
            ) as lead_effective_from

    from {{ model }} as s
)
select * from services_versions
where effective_to < lead_effective_from 
    and lead_effective_from is not null

{% endtest%}