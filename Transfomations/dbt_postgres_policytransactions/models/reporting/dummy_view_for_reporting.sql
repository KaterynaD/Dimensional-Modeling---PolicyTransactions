select 
f.month_id as year_month,
d.policystate as statecd,
sum(f.metric) as metric
from {{ ref('dummy_agg_table') }} f 
join {{ ref('dim_policy') }} d 
on f.policy_id=d.policy_id
group by
f.month_id,
d.policystate