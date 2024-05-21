{% macro default_dim_driver() %}

select
'Unknown'::varchar(100) driver_id,
'1900-01-01'::timestamp without time zone valid_fromdate,
'3000-01-01'::timestamp without time zone valid_todate,
'Unknown' driver_uniqueid,
'~'::varchar(10) gendercd,
'1900-01-01'::varchar(10) birthdate,
'~'::varchar(10) maritalstatuscd,
0::varchar(3) pointscharged,
{{ loaddate() }}
union all

{% endmacro %}
