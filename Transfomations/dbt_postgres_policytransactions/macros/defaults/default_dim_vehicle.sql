{% macro default_dim_vehicle() %}

select
'Unknown' vehicle_id,
'1900-01-01' valid_fromdate,
'3000-12-31' valid_todate,
'Unknown' vehicle_uniqueid,
'~' vin,
'~' model,
'~' modelyr,
'~' manufacturer,
'0' estimatedannualdistance,
{{ loaddate() }}
union all

{% endmacro %}