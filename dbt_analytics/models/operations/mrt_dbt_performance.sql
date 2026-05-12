{{ config(
    materialized='table'
) }}

with parsed_logs as (
    select
        user_name,
        duration_seconds,
        start_time,
        -- Измененная регулярка: 
        -- (?i) - регистронезависимость
        -- (?:...) - ключевые слова
        -- \\s+ - пробелы
        -- (.*?) - захватываем ВСЁ (включая точки и кавычки) до тех пор, пока не встретим пробел или конец строки
        -- Мы ищем паттерн, который начинается с кавычки и заканчивается кавычкой
        regexp_extract(query_text, '(?i)(?:create|insert|into|merge|update)\\s+.*?(`[^`]+`.`[^`]+`.`[^`]+`)', 1) as raw_model_path,
        query_text
    from {{ ref('fct_dbt_audit_logs') }}
),

fallback_parsing as (
    select 
        user_name,
        duration_seconds,
        start_time,
        -- Если первый вариант (3 сегмента) не сработал, пробуем забрать хотя бы то, что есть
        coalesce(
            raw_model_path, 
            regexp_extract(query_text, '(?i)(?:create|insert|into|merge|update)\\s+.*?(`[^`\\s]+`)', 1)
        ) as final_raw_path
    from parsed_logs
),

cleaned_models as (
    select
        user_name,
        -- Убираем технические суффиксы из полного пути
        regexp_replace(final_raw_path, '__dbt_tmp|__dbt_backup', '') as model_full_path,
        duration_seconds,
        start_time
    from fallback_parsing
    where final_raw_path is not null and final_raw_path != ''
)

select
    user_name,
    model_full_path as model_name,
    count(*) as total_runs,
    round(sum(duration_seconds), 2) as total_duration_seconds,
    round(avg(duration_seconds), 2) as avg_duration,
    max(start_time) as last_run_at
from cleaned_models
where 
    -- Исключаем CI-схемы и схемы разработчиков
    model_full_path not ilike '%ci_schema%' 
    and model_full_path not ilike '%dev_%'
    -- Оставляем только те, что относятся к основному каталогу prod
    and model_full_path ilike '%microservices_prod%'
group by 1, 2
order by total_duration_seconds desc
