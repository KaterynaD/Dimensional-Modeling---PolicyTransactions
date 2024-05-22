{{ config(
   materialized='incremental',
   unique_key=['driver_id'],
   pre_hook='{{ dbt_log_insert() }}',
   post_hook='{{ dbt_log_update() }}'   
          ) 
  }}

{% if  var('load_defaults')   %}

{{ default_dim_driver() }}

{% endif %}

/*dummy select for linage only*/
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
from {{ ref('stg_driver') }} stg
where 1=2 