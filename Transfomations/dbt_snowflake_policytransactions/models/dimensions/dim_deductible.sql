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
coalesce(stg.deductible1, 0) as deductible1,
coalesce(stg.deductible2, 0) as deductible2
from {{ source('PolicyStats', 'stg_pt') }} stg

{% if is_incremental() %}

where {{ incremental_condition() }}
and  not exists (select 1 from {{ this }} dim where concat(cast(cast(stg.deductible1 as int) as varchar), '_', cast(cast(stg.deductible2 as int)as varchar))=concat(cast(cast(dim.deductible1 as int) as varchar), '_', cast(cast(dim.deductible2 as int)as varchar)))

{% else %}

where  {{ full_load_condition() }}
and  (stg.deductible1<>0 and stg.deductible2<>0) /*default which we can not catch when dim does not exist*/

{% endif %}


)



, data as (

{% if  var('load_defaults')   %}

{{ default_dim_deductible() }}

{% endif %}

select
ROW_NUMBER() OVER(order by deductible1, deductible2) deductible_id,
deductible1,
deductible2,
{{ loaddate() }}
from stg_data
)
select
data.deductible_id::integer as deductible_id,
data.deductible1::numeric as deductible1,
data.deductible2::numeric as deductible2,
data.loaddate::timestamp without time zone as loaddate
from data



