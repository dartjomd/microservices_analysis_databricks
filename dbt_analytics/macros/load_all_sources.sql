{% macro load_all_sources() %}

  -- Load services lookup
  {{ load_source(
      source_name = 'services_lookup_raw',
      s3_path = 's3://de-practice-artjom-s3/services',
      file_format = 'CSV',
      columns = {
          'service_name': 'string',
          'service_category': 'string',
          'criticality_tier': 'string'
      }
  ) }}

  -- Load microservices logs
  {{ load_source(
      source_name = 'microservices_logs_raw',
      s3_path = 's3://de-practice-artjom-s3/landing',
      file_format = 'JSON',
      columns = {
          'latency_ms': 'bigint',
          'service_name': 'string',
          'status_code': 'string',
          'timestamp': 'string'
      }
  ) }}

{% endmacro %}