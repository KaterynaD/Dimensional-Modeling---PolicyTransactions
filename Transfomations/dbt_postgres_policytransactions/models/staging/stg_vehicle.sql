
select distinct
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) , '')
         || '|' || coalesce(cast(stg.veh_effectivedate::timestamp without time zone as varchar ), '')
        )  vehicle_id,
stg.veh_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) vehicle_uniqueid,
stg.vin,
stg.model,
stg.modelyr,
stg.manufacturer
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}