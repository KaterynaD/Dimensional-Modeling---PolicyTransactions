{{ config(materialized='view') }}

select distinct
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) , '')
         || '|' || coalesce(cast(stg.drv_effectivedate::timestamp without time zone as varchar ), '')
        )  driver_id,
stg.drv_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) driver_uniqueid,
stg.gendercd,
to_char(stg.birthdate,'yyyy-mm-dd') as birthdate,
stg.maritalstatuscd,
cast(stg.pointschargedterm as varchar) as pointscharged
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}

