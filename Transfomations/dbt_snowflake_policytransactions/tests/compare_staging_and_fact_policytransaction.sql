with staging as 
(
select count(*) cnt ,
sum(stg.amount) sum_amount
from {{ source('PolicyStats', 'stg_pt') }} stg
where  {{ incremental_condition() }}
)
, fact as 
(
select count(*) cnt  ,
sum(amount) sum_amount
from {{ ref('fact_policytransaction') }} 
where {{ loaddate_in_where() }}
)
select
staging.cnt as cnt_in_staging,
fact.cnt as cnt_in_fact,
staging.sum_amount as sum_amount_in_staging,
fact.sum_amount as sum_amount_in_fact
from staging
join fact
on 1=1
where 
staging.cnt <> fact.cnt
or 
staging.sum_amount <> fact.sum_amount