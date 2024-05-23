{{ config(materialized='view') }}

with data as (
/*There is a small amount of data issues in the staging data related to different attributes for the same uniqueid and transactioneffective date.
  No way to automatically select a proper record. The issue is in the source system processing XML data.
  Any record can be valid. In this case I select just max attribute value for simplicity.
*/
select 
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) , '')
         || '|' || coalesce(cast(stg.veh_effectivedate::timestamp without time zone as varchar ), '')
        )  vehicle_id,
stg.veh_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) vehicle_uniqueid,
max(stg.vin) as  vin,
max(stg.model) as model,
max(stg.modelyr) as modelyr,
max(stg.manufacturer) as manufacturer,
max(stg.estimatedannualdistance) as estimatedannualdistance
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
group by
stg.veh_effectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) )
)
select
data.vehicle_id::varchar(100) as vehicle_id,
data.transactioneffectivedate::timestamp without time zone as transactioneffectivedate,
data.vehicle_uniqueid::varchar(100) as vehicle_uniqueid,
data.vin::varchar(20) as vin,
data.model::varchar(100) as model,
data.modelyr::varchar(100) as modelyr,
data.manufacturer::varchar(100) as manufacturer,
data.estimatedannualdistance::varchar(100) as estimatedannualdistance
from data