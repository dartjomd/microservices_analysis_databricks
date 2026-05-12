{{ config(
    materialized='table'
) }}

with parsed_logs as (
    select
        pl.user_name,
        pl.duration_seconds,
        pl.start_time,
        pl.query_text,
        regexp_extract(
            pl.query_text,
            '(?i)(?:create|insert|into|merge|update)\\s+.*?(`[^`]+`.`[^`]+`.`[^`]+`)', -- noqa: LT05
            1
        ) as raw_model_path
    from {{ ref('fct_dbt_audit_logs') }} as pl
),

fallback_parsing as (
    select
        fb.user_name,
        fb.duration_seconds,
        fb.start_time,
        coalesce(
            fb.raw_model_path,
            regexp_extract(
                fb.query_text,
                '(?i)(?:create|insert|into|merge|update)\\s+.*?(`[^`\\s]+`)',
                1
            )
        ) as final_raw_path
    from parsed_logs as fb
),

cleaned_models as (
    select
        cm.user_name,
        cm.duration_seconds,
        cm.start_time,
        regexp_replace(
            cm.final_raw_path,
            '__dbt_tmp|__dbt_backup',
            ''
        ) as model_full_path
    from fallback_parsing as cm
    where
        cm.final_raw_path is not null
        and cm.final_raw_path != ''
)

select
    user_name,
    model_full_path as model_name,
    max(start_time) as last_run_at,
    count(*) as total_runs,
    round(sum(duration_seconds), 2) as total_duration_seconds,
    round(avg(duration_seconds), 2) as avg_duration
from cleaned_models
where
    model_full_path not ilike '%ci_schema%'
    and model_full_path not ilike '%dev_%'
    and model_full_path ilike '%microservices_prod%'
group by user_name, model_name
order by total_duration_seconds desc
