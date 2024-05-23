{{ config(materialized='view') }}

with data as (
/*There is a small amount of data issues in the staging data related to different attributes for the same uniqueid and transactioneffective date.
  No way to automatically select a proper record. The issue is in the source system processing XML data.
  Any record can be valid. In this case I select just max attribute value for simplicity.
*/        
select 
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) , '')
         || '|' || coalesce(cast(stg.drv_effectivedate::timestamp without time zone as varchar ), '')
        )  driver_id,
stg.drv_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) driver_uniqueid,
max(stg.gendercd) as gendercd,
max(to_char(stg.birthdate,'yyyy-mm-dd')) as birthdate,
max(stg.maritalstatuscd) as maritalstatuscd,
max(cast(stg.pointschargedterm as varchar)) as  pointscharged
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
group by 
stg.drv_effectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) 
)
select
data.driver_id::varchar(100) as driver_id,
data.transactioneffectivedate::timestamp without time zone as transactioneffectivedate,
data.driver_uniqueid::varchar(100) as driver_uniqueid,
data.gendercd::varchar(10) as gendercd,
data.birthdate::varchar(10) as birthdate,
data.maritalstatuscd::varchar(10) as maritalstatuscd,
data.pointscharged::varchar(3) as pointscharged
from data

