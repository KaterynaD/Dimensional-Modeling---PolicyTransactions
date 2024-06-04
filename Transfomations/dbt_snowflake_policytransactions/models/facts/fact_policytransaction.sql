{{ config(materialized='incremental',
   unique_key=['policytransaction_id'],
   pre_hook='{{ dbt_log_insert() }}',
   post_hook='{{ dbt_log_update() }}') }}

with data as (
select 
/*-----------------------------------------------------------------------*/
{{ dbt_utils.generate_surrogate_key([
                'transactiondate'
            ])
        }} as policytransaction_id,
/*-----------------------------------------------------------------------*/        
stg.transactiondate,
stg.accountingdate,
/*-----------------------------------------------------------------------*/
coalesce(dim_transaction.transaction_id,0) as transaction_id,
stg.transactioneffectivedate,
stg.transactionsequence,
/*-----------------------------------------------------------------------*/
coalesce(dim_policy.policy_id, 0) as policy_id,
stg.veh_effectivedate,
coalesce(dim_vehicle.vehicle_id, 'Unknown') as vehicle_id,
coalesce(dim_driver.driver_id, 'Unknown') as driver_id,
coalesce(dim_coverage.coverage_id, 'Unknown') as coverage_id,
coalesce(dim_limit.limit_id, 0) as limit_id,
coalesce(dim_deductible.deductible_id, 0) as deductible_id,
/*-----------------------------------------------------------------------*/
stg.amount
/*-----------------------------------------------------------------------*/
/*=======================================================================*/
from {{ source('PolicyStats', 'stg_pt') }} stg
--
left outer join {{ ref('dim_transaction') }} dim_transaction
on stg.transactioncd=dim_transaction.transactioncd
--
left outer join {{ ref('dim_policy') }} dim_policy
on stg.policy_uniqueid=dim_policy.policy_uniqueid
--
left outer join {{ ref('dim_coverage') }} dim_coverage
on stg.coveragecd=dim_coverage.coveragecd
and stg.subline=dim_coverage.subline
and stg.asl=dim_coverage.asl
--
left outer join {{ ref('dim_limit') }} dim_limit
on stg.limit1=dim_limit.limit1
and stg.limit2=dim_limit.limit2
--
left outer join {{ ref('dim_deductible') }} dim_deductible
on stg.deductible1=dim_deductible.deductible1
and stg.deductible2=dim_deductible.deductible2
--
left outer join {{ ref('dim_driver') }} dim_driver
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) = dim_driver.driver_uniqueid
and (stg.drv_effectivedate >= dim_driver.valid_fromdate 
and stg.drv_effectivedate < dim_driver.valid_todate)

--
left outer join {{ ref('dim_vehicle') }} dim_vehicle
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) = dim_vehicle.vehicle_uniqueid
and (stg.veh_effectivedate >= dim_vehicle.valid_fromdate 
and stg.veh_effectivedate < dim_vehicle.valid_todate)
/*=======================================================================*/
{% if is_incremental() %}

where {{ incremental_condition() }}

{% else %}

where  {{ full_load_condition() }}

{% endif %}



)
select
/*-----------------------------------------------------------------------*/
data.policytransaction_id::varchar(100) as policytransaction_id,
/*-----------------------------------------------------------------------*/        
data.transactiondate::int as transactiondate,
data.accountingdate::int as accountingdate,
/*-----------------------------------------------------------------------*/
data.transaction_id::int as transaction_id,
data.transactioneffectivedate::int as transactioneffectivedate,
data.transactionsequence::int as transactionsequence,
/*-----------------------------------------------------------------------*/
data.policy_id::int as policy_id,
data.veh_effectivedate,
data.vehicle_id::varchar(100) as vehicle_id,
data.driver_id::varchar(100) as driver_id,
data.coverage_id::varchar(100) as coverage_id,
data.limit_id::int as limit_id,
data.deductible_id::int as deductible_id,
/*-----------------------------------------------------------------------*/
data.amount::numeric as amount,
{{ loaddate() }}
from data