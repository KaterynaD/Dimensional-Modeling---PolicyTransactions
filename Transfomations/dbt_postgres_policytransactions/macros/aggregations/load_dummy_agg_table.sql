 {% macro load_dummy_agg_table() %} 

 /*
 assuming complex transformations with several temporary tables
 this is just a demo placeholder
 */

{% set delete_dummy_agg_data %}

 delete from {{ ref('dummy_agg_table') }} where month_id=(select max(cast(substring(cast( t.accountingdate as varchar), 1,6) as int)) from {{ ref('fact_policytransaction') }} t)

{% endset %}

{% set insert_dummy_agg_data %}


insert into {{ ref('dummy_agg_table') }} 
(
agg_id, 
month_id,
policy_id,
metric,
loaddate
)
select
cast(substring(cast( accountingdate as varchar), 1,6) as int) agg_id,
cast(substring(cast( accountingdate as varchar), 1,6) as int) month_id,
policy_id,
sum(amount) as metric,
{{ loaddate() }}
from 
{{ ref('fact_policytransaction') }}
where cast(substring(cast( accountingdate as varchar), 1,6) as int)= (select max(cast(substring(cast( t.accountingdate as varchar), 1,6) as int)) from {{ ref('fact_policytransaction') }} t)
group by
cast(substring(cast( accountingdate as varchar), 1,6) as int),
policy_id;

{% endset %}

{% set run_operation =  'load_dummy_agg_table:dummy_agg_table'   %}

{{ dbt_log_insert(run_operation) }}

{% do run_query(delete_dummy_agg_data) %}

{% do run_query(insert_dummy_agg_data) %}

{{ dbt_log_update(run_operation) }}

{% endmacro %}