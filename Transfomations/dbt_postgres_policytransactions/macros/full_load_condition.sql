{% macro full_load_condition() %}
    stg.transactiondate='{{ var('current_date' ) }}'
{% endmacro %}