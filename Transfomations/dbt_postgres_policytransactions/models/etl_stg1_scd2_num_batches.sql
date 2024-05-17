with original_raw_data as (select distinct
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) risk_uniqueid,
veh_effectivedate
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
and vin='927079242285108675'
)
,raw_data as (select
risk_uniqueid,
veh_effectivedate,
row_number() over(partition by risk_uniqueid  order by veh_effectivedate) rn
from original_raw_data
)
select
'stg_risk' table_name,
max(rn) num_batches
from raw_data