{{ config(materialized='incremental',
  unique_key=['dbt_scd_id']) }}

select distinct
stg.policy_uniqueid,
stg.transactioneffectivedate,
stg.risk_uniqueid,
stg.vin,
stg.model,
stg.modelyr,
stg.manufacturer,
stg.loaddate,
md5(coalesce(cast(stg.risk_uniqueid as varchar ), '')
         || '|' || coalesce(cast(stg.transactioneffectivedate::timestamp without time zone as varchar ), '')
        ) dbt_scd_id, 
stg.dbt_updated_at,
stg.transactioneffectivedate dbt_valid_from,
stg.transactioneffectivedate dbt_valid_to
from {{ref('stg_risk')}} stg
left outer join {{ref('dim_risk_io')}} dim
on md5(coalesce(cast(stg.risk_uniqueid as varchar ), '')
         || '|' || coalesce(cast(stg.transactioneffectivedate::timestamp without time zone as varchar ), '')
        ) = dim.dbt_scd_id
where dim.dbt_scd_id is null
{% if is_incremental() %}

and md5(coalesce(cast(stg.risk_uniqueid as varchar ), '')
         || '|' || coalesce(cast(stg.transactioneffectivedate::timestamp without time zone as varchar ), '')
        ) not in (select e.dbt_scd_id from {{ this }} e)

{% endif %}
