{% macro loaddate() %}
    '{{ var('loaddate' ) }}'::timestamp without time zone loaddate
{% endmacro %}