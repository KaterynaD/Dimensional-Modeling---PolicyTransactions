{{ config(materialized='incremental',
  unique_key=['deductible_id'],
  pre_hook='{{ dbt_log_insert() }}',
  post_hook='{{ dbt_log_update() }}') }}



with 
{% if is_incremental() %}

max_deductible as (select max(deductible_id) id from {{this}}),

{% else %}

max_deductible as (select 0 id ),

{% endif %}
stg_data as (
select distinct
stg.deductible1,
stg.deductible2
from {{ source('PolicyStats', 'stg_pt') }} stg

{% if is_incremental() %}

where {{ incremental_condition() }}

{% else %}

where  {{ full_load_condition() }}

{% endif %}


)

{% if  var('load_defaults')   %}

{{ default_dim_deductible() }}

{% endif %}

select
ROW_NUMBER() OVER(order by deductible1, deductible2) deductible_id,
deductible1,
deductible2,
{{ loaddate() }}
from stg_data



