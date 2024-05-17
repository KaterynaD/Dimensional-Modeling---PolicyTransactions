{% macro default_dim_limit() %}

select
0 limit_id,
'~' limit1,
'~' limit2,
{{ loaddate() }}
union all

{% endmacro %}