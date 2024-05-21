{{ config(materialized='incremental',
  unique_key=['vehicle_id']) }}

{% if  var('load_defaults')   %}

{{ default_dim_vehicle() }}

{% endif %}

/*dummy select for linage only*/
select
'Unknown'::varchar(100) vehicle_id,
'1900-01-01'::timestamp without time zone valid_fromdate,
'3000-01-01'::timestamp without time zone valid_todate,
'Unknown' vehicle_uniqueid,
'~'::varchar(20) vin,
'~'::varchar(100) model,
'~'::varchar(100) modelyr,
'~'::varchar(100) manufacturer,
{{ loaddate() }}
from {{ ref('stg_vehicle') }} stg
where 1=2 