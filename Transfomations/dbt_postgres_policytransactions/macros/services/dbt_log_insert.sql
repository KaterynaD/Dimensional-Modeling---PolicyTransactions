{% macro dbt_log_insert(operation) %} 


    


 {% set insert_startdate_operation %}

 {% set run_at_date = get_run_at_date() %}

{% if operation|length > 1  %} 
  
  {% set run_operation =  operation  %}

{% else %} 
 
  {% set run_operation =   this   %}

{% endif %}
 
 INSERT INTO {{ ref('dbt_log') }} (startdate, operation) VALUES ('{{ run_at_date }}', '{{ run_operation }}');


 {% endset %}

{% do run_query(insert_startdate_operation) %}

{% endmacro %}