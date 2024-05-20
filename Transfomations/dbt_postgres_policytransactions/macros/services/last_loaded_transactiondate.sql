 {% macro last_loaded_transactiondate() %} 

 {% set last_loaded_transactiondate %}

 SELECT transactiondate FROM {{ ref('etl_log') }} WHERE LoadDate=(select max(loaddate) from {{ ref('etl_log') }} where endloaddate is not null)

 {% endset %}

{% do run_query(last_loaded_transactiondate) %}

{% endmacro %}