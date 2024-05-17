{% macro loaddate() %}
    to_date('{{ var('loaddate' ) }}', 'yyyy-mm-dd') loaddate
{% endmacro %}