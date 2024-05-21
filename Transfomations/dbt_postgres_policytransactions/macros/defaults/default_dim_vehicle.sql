{% macro default_dim_vehicle() %}

select
'Unknown'::varchar(100) vehicle_id,
'1900-01-01'::timestamp without time zone valid_fromdate,
'3000-01-01'::timestamp without time zone valid_todate,
0::integer policy_uniqueid,
'Unknown' vehicle_uniqueid,
'~'::varchar(20) vin,
'~'::varchar(100) model,
'~'::varchar(100) modelyr,
'~'::varchar(100) manufacturer,
{{ loaddate() }}
union all

{% endmacro %}