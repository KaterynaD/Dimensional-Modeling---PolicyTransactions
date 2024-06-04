{% macro incremental_condition() %}
    stg.transactiondate>'{{ var('latest_loaded_transactiondate' ) }}' and stg.transactiondate<='{{ var('new_transactiondate' ) }}'
{% endmacro %}