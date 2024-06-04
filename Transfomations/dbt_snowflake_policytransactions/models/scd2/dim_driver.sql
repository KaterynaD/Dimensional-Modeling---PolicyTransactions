{{ config(
   materialized='incremental',
   unique_key=['driver_id'],
   pre_hook='{{ dbt_log_insert() }}',
   post_hook='{{ dbt_log_update() }}'   
          ) 
  }}

with data as (
{% if  var('load_defaults')   %}

{{ default_dim_driver() }}

{% endif %}

/*dummy select for linage only*/
select
'Unknown' driver_id,
'1900-01-01' valid_fromdate,
'3000-01-01' valid_todate,
'Unknown' driver_uniqueid,
'~' gendercd,
'1900-01-01' birthdate,
'~' maritalstatuscd,
'0' pointscharged,
{{ loaddate() }}
from {{ ref('stg_driver') }} stg
where 1=2 
)
select
data.driver_id::varchar(100) as driver_id,
data.valid_fromdate::timestamp without time zone as valid_fromdate,
data.valid_todate::timestamp without time zone as valid_todate,
data.driver_uniqueid::varchar(100) as driver_uniqueid,
data.gendercd::varchar(10) as gendercd,
data.birthdate::varchar(10) as birthdate,
data.maritalstatuscd::varchar(10) as maritalstatuscd,
data.pointscharged::varchar(3) as pointscharged,
data.loaddate::timestamp without time zone as loaddate
from data