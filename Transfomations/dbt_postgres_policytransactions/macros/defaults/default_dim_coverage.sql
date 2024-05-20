{% macro default_dim_coverage() %}

select
'Unknown' coverage_id,
'~' coveragecd,
'~' subline,
'~' asl,
{{ loaddate() }}
union all

{% endmacro %}