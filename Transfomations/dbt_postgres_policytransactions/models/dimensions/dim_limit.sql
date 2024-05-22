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

{% else %}

where  {{ full_load_condition() }}

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

