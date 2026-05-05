{% macro load_source(source_name, s3_path, file_format, columns) %}

{% set raw_schema = get_raw_schema('bronze') %}
{% set table_name = raw_schema ~ "." ~ source_name %}

-- Create schema
{% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ raw_schema) %}

-- Create list of "col type" items
{% set col_list = [] %}
{% for col, type in columns.items() %}
    {% do col_list.append(col ~ " " ~ type) %}
{% endfor %}

-- Create table using col_list to extract fields and their types
{% do run_query("
    CREATE TABLE IF NOT EXISTS " ~ table_name ~ " (
        " ~ col_list | join(', ') ~ ",
        _file_source_name string,
        _ingested_at timestamp
    ) USING DELTA
") %}

-- Define copy into to insert data into created table query
{% set copy_query %}
    COPY INTO {{ table_name }}
    FROM (
        SELECT 
            {% for col in columns.keys() -%}
            {{ col }},
            {% endfor -%}
            _metadata.file_path AS _file_source_name,
            current_timestamp() AS _ingested_at
        FROM '{{ s3_path }}'
    )
    FILEFORMAT = {{ file_format }}
    {% if file_format | lower == 'csv' %}
    FORMAT_OPTIONS ('header' = 'true', 'inferschema' = 'false')
    {% endif %}
    COPY_OPTIONS (
        'mergeSchema' = 'true'
        
        -- If CI, load only 5 files AND limit its length
        {% if target.name == 'ci' %}
        , 'maxFiles' = '5'
        {% endif %}
    )
{% endset %}

-- Run copy into query
{% do run_query(copy_query) %}
{% do log("Successfully loaded data from S3 from " ~ s3_path ~ " into " ~ table_name, info=True) %}

{% endmacro %}