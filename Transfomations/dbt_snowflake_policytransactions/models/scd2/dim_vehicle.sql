{{ config(materialized='incremental',
   unique_key=['vehicle_id'],
   pre_hook='{{ dbt_log_insert() }}',
   post_hook='{{ dbt_log_update() }}') }}

with data as (
{% if  var('load_defaults')   %}

{{ default_dim_vehicle() }}

{% endif %}

/*dummy select for linage only*/
select
'Unknown' vehicle_id,
'1900-01-01' valid_fromdate,
'3000-01-01' valid_todate,
'Unknown' vehicle_uniqueid,
'~' vin,
'~' model,
'~' modelyr,
'~' manufacturer,
'0' estimatedannualdistance,
{{ loaddate() }}
from {{ ref('stg_vehicle') }} stg
where 1=2 
)
select
vehicle_id::varchar(100) as vehicle_id,
valid_fromdate::timestamp without time zone as valid_fromdate,
valid_todate::timestamp without time zone as valid_todate,
vehicle_uniqueid::varchar(100) as vehicle_uniqueid,
vin::varchar(20) as vin,
model::varchar(100) as model,
modelyr::varchar(100) as modelyr,
manufacturer::varchar(100) as manufacturer,
estimatedannualdistance::varchar(100) as estimatedannualdistance,
data.loaddate::timestamp without time zone as loaddate
from data