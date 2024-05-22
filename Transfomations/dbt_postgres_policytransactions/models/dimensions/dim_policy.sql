{{ config(materialized='incremental',
  unique_key=['policy_id'],
  pre_hook='{{ dbt_log_insert() }}',
  post_hook='{{ dbt_log_update() }}') }}

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

{% if is_incremental() %}

where {{ incremental_condition() }}

{% else %}

where  {{ full_load_condition() }}

{% endif %}

