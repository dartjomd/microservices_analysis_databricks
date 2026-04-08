{% test model_freshness(model, column_name, max_hours) %}

select max({{column_name}}) as last_record
from {{model}}
having last_record < current_timestamp() - interval {{max_hours}} hours

{% endtest %}