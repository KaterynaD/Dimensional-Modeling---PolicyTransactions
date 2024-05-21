 {% macro back_dated_scd2_records_insert() %} 

 {% set update_prev_record_valid_to %}

with new_data as (
select distinct
stg.risk_uniqueid,
stg.transactioneffectivedate dbt_valid_from
from {{ref('stg_risk')}} stg
left outer join {{ref('dim_risk')}} dim
on md5(coalesce(cast(stg.risk_uniqueid as varchar ), '')
         || '|' || coalesce(cast(stg.transactioneffectivedate::timestamp without time zone as varchar ), '')
        ) = dim.dbt_scd_id
where dim.dbt_scd_id is null
and stg.{{ loaddate_in_where() }}
)
,prev_record as (
select dim.risk_uniqueid,
max(dim.dbt_valid_from) dbt_valid_to
from {{ref('dim_risk')}} dim
join new_data
on dim.risk_uniqueid=new_data.risk_uniqueid
and dim.dbt_valid_from<new_data.dbt_valid_from
group by dim.risk_uniqueid
)
update {{ref('dim_risk')}}
set
 dbt_valid_to=new_data.dbt_valid_from
from new_data
join prev_record 
on new_data.risk_uniqueid=prev_record.risk_uniqueid
where {{ref('dim_risk')}}.dbt_scd_id =  md5(coalesce(cast(prev_record.risk_uniqueid as varchar ), '')
         || '|' || coalesce(cast(prev_record.dbt_valid_to::timestamp without time zone as varchar ), '')
        );

{% endset %}

{% do run_query(update_prev_record_valid_to) %}

{% set insert_new %}

with new_data as (
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
stg.transactioneffectivedate dbt_valid_from
from {{ref('stg_risk')}} stg
left outer join {{ref('dim_risk')}} dim
on md5(coalesce(cast(stg.risk_uniqueid as varchar ), '')
         || '|' || coalesce(cast(stg.transactioneffectivedate::timestamp without time zone as varchar ), '')
        ) = dim.dbt_scd_id
where dim.dbt_scd_id is null
and stg.{{ loaddate_in_where() }}
)
,next_record as (
select dim.risk_uniqueid,
min(dim.dbt_valid_from) dbt_valid_to
from {{ref('dim_risk')}} dim
join new_data
on dim.risk_uniqueid=new_data.risk_uniqueid
and dim.dbt_valid_from>new_data.dbt_valid_from
group by dim.risk_uniqueid
)
insert into {{ref('dim_risk')}}
select
new_data.policy_uniqueid,
new_data.transactioneffectivedate,
new_data.risk_uniqueid,
new_data.vin,
new_data.model,
new_data.modelyr,
new_data.manufacturer,
new_data.loaddate,
new_data.dbt_scd_id, 
new_data.dbt_updated_at,
new_data.dbt_valid_from,
next_record.dbt_valid_to
from new_data
join next_record 
on new_data.risk_uniqueid=next_record.risk_uniqueid;

{% endset %}

{% do run_query(insert_new) %}

{% endmacro %}