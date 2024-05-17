{% macro incremental_condition() %}
    stg.transactiondate>'{{ var('latest_loaded_date' ) }}' and stg.transactiondate<='{{ var('current_date' ) }}'
{% endmacro %}