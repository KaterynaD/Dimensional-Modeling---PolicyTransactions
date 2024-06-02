{% macro default_dim_driver() %}

select
'Unknown' driver_id,
'1900-01-01' valid_fromdate,
'3000-01-01' valid_todate,
'Unknown' driver_uniqueid,
'~' gendercd,
'1900-01-01' birthdate,
'~' maritalstatuscd,
0 pointscharged,
{{ loaddate() }}
union all

{% endmacro %}
