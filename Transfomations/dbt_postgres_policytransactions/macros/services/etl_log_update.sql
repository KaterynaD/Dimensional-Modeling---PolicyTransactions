 {% macro etl_log_update() %} 

 {% set update_endloaddate %}

 UPDATE {{ ref('etl_log') }}
 SET endloaddate='{{ var('endloaddate' ) }}'
 WHERE loaddate='{{ var('loaddate' ) }}';

 {% endset %}

{% do run_query(update_endloaddate) %}

{% endmacro %}