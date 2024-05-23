{{ config(materialized='incremental',
  unique_key=['policy_id'],
  pre_hook='{{ dbt_log_insert() }}',
  post_hook='{{ dbt_log_update() }}') }}



/*
The data are not perfect in the staging from XML-based source system.
Sometimes, there are different attributes for the same uniqueid in one daily batch.
DBT approach does not eliminate such records because they are in 1 batch.
There is no proper way to understand which record is correct in an automated mode.
I select just one, latest transaction record per batch: the max transactionsequencenumber
*/

with most_latest_data as 
(
select 

policy_uniqueid,
max(transactionsequence) max_transactionsequence

from {{ source('PolicyStats', 'stg_pt') }} stg

{% if is_incremental() %}

where {{ incremental_condition() }}

{% else %}

where  {{ full_load_condition() }}

{% endif %}

group by policy_uniqueid
)
, data as (

{% if  var('load_defaults')   %}

{{ default_dim_policy() }}

{% endif %}

select distinct
stg.policy_uniqueid policy_id,
stg.policy_uniqueid,
stg.policynumber,
stg.effectivedate,
stg.expirationdate,
stg.inceptiondate,
stg.policystate,
stg.carriercd,
stg.companycd,
{{ loaddate() }}
from {{ source('PolicyStats', 'stg_pt') }} stg
join most_latest_data
on stg.policy_uniqueid = most_latest_data.policy_uniqueid
and stg.transactionsequence = most_latest_data.max_transactionsequence
)
select
data.policy_id::integer as policy_id,
data.policy_uniqueid::integer as policy_uniqueid,
data.policynumber::varchar(20) as policynumber,
data.effectivedate::date as effectivedate,
data.expirationdate::date as expirationdate,
data.inceptiondate::date as inceptiondate,
data.policystate::varchar(2) as policystate,
data.carriercd::varchar(5) as carriercd,
data.companycd::varchar(5) as companycd,
data.loaddate::timestamp without time zone as loaddate
from data

