{% macro loaddate_in_where() %}
    loaddate = to_date('{{ var('loaddate' ) }}', 'yyyy-mm-dd')
{% endmacro %}