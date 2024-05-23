{{ config(materialized='incremental',
  unique_key=['coverage_id'],
  pre_hook='{{ dbt_log_insert() }}',
  post_hook='{{ dbt_log_update() }}')}}

with data as (
{% if  var('load_defaults')   %}

{{ default_dim_coverage() }}

{% endif %}

select distinct
{{ dbt_utils.generate_surrogate_key([
                'coveragecd', 
                'subline',
                'asl'
            ])
        }} coverage_id,
stg.coveragecd,
stg.subline,
stg.asl,
{{ loaddate() }}
from {{ source('PolicyStats', 'stg_pt') }} stg

{% if is_incremental() %}

where {{ incremental_condition() }}

{% else %}

where  {{ full_load_condition() }}

{% endif %}
)
select
data.coverage_id,
data.coveragecd,
data.subline,
data.asl,
data.loaddate
from data

