{{ config(materialized='incremental',
  unique_key=['limit_id'],
  pre_hook='{{ dbt_log_insert() }}',
  post_hook='{{ dbt_log_update() }}') }}



with stg_data as (
select distinct
stg.limit1,
stg.limit2
from {{ source('PolicyStats', 'stg_pt') }} stg

{% if is_incremental() %}

where {{ incremental_condition() }}
and  not exists (select 1 from {{ this }} dim where concat(cast(stg.limit1 as varchar), '_', cast(stg.limit2 as varchar))=concat(cast(dim.limit1 as varchar), '_', cast(dim.limit2 as varchar)))

{% else %}

where  {{ full_load_condition() }}
and  (stg.limit1<>'~' and stg.limit2<>'~') /*default which we can not catch when dim does not exist*/

{% endif %}



)

{% if  var('load_defaults')   %}

{{ default_dim_limit() }}

{% endif %}

select
{{ increment_sequence() }} limit_id,
limit1,
limit2,
{{ loaddate() }}
from stg_data

