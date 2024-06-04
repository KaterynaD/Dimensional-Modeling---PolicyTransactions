{% macro full_load_condition() %}
    stg.transactiondate='{{ var('new_transactiondate' ) }}'
{% endmacro %}