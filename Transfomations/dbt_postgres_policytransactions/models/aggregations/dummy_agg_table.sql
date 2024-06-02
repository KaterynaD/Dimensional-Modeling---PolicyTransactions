{{ config(
   materialized='incremental',
   unique_key=['agg_id'],
   pre_hook='{{ dbt_log_insert() }}',
   post_hook='{{ dbt_log_update() }}'   
          ) 
  }}

with data as (
/*dummy select for linage only*/
select
0 agg_id,
190001 month_id,
0 policy_id,
0.00 metric,
{{ loaddate() }}
from {{ ref('fact_policytransaction') }} stg
where 1=2 
)
select
data.agg_id::int as agg_id,
data.month_id::int as month_id,
data.policy_id::int as policy_id,
data.metric::numeric as metric,
data.loaddate::timestamp without time zone as loaddate
from data