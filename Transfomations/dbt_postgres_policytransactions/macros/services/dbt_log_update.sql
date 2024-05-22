{% macro dbt_log_update(operation) %} 


    


 {% set update_enddate_operation %}

 {% set run_at_date = get_run_at_date() %}


{% if operation|length > 1  %} 
  
  {% set run_operation =  operation  %}

{% else %} 
 
  {% set run_operation =   this   %}

{% endif %}


 
 UPDATE {{ ref('dbt_log') }}
 SET enddate='{{ run_at_date }}'
 WHERE OPERATION='{{ run_operation }}'
 and enddate is null
 and startdate =(select max(t.startdate) from  {{ ref('dbt_log') }} t where t.OPERATION='{{ run_operation }}' and t.enddate is null)
 ;


 {% endset %}

{% do run_query(update_enddate_operation) %}

{% endmacro %}