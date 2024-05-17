{% snapshot stg_risk %}



{{
    config(
      target_database='airflow',
      target_schema='policy_trn',
      unique_key='risk_uniqueid',
      strategy='check',
      check_cols=['model','modelyr','manufacturer'],
    )
}}

with original_data as (select distinct
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) risk_uniqueid,
veh_effectivedate
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
)
,data as (select
risk_uniqueid,
veh_effectivedate,
row_number() over(partition by risk_uniqueid  order by veh_effectivedate) rn
from original_data
)
select distinct
stg.policy_uniqueid,
stg.veh_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) risk_uniqueid,
stg.vin,
stg.model,
stg.modelyr,
stg.manufacturer,
{{ loaddate() }}
from {{ source('PolicyStats', 'stg_pt') }} stg
join data
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) )=data.risk_uniqueid
and stg.veh_effectivedate=data.veh_effectivedate
where {{ incremental_condition() }}
and data.rn={{ var('batch_num' ) }}





{% endsnapshot %}