 {% macro load_scd2() %} 

 {% set insert_changed_data %}

/*-------------------Forward Dated Changes-------------------------*/


/*only new changed data*/
create temporary table new_data as
with stg_data as (
/*--new data in stage can have changes for the same uniqueid - historical load as an example --*/
select distinct
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) , '')
         || '|' || coalesce(cast(stg.veh_effectivedate::timestamp without time zone as varchar ), '')
        )  vehicle_id,
stg.policy_uniqueid,
stg.veh_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.vin as varchar) ) vehicle_uniqueid,
stg.vin,
stg.model,
stg.modelyr,
stg.manufacturer
from {{ source('PolicyStats', 'stg_pt') }} stg
where {{ incremental_condition() }}
and vin='927079242285108675')
/*-- we need existing data from dim to compare with new add/not add with/without changes --*/
, existing_data as (
select
dim.vehicle_id,
dim.policy_uniqueid,
dim.valid_fromdate transactioneffectivedate,
dim.vehicle_uniqueid,
dim.vin,
dim.model,
dim.modelyr,
dim.manufacturer
from {{ ref('dim_vehicle') }} dim
where dim.vehicle_uniqueid in (select stg.vehicle_uniqueid from stg_data stg)
)
,data as (
/*---new data ---*/
select stg.* ,
1 new_data
from stg_data stg 
left outer join existing_data dim 
on stg.vehicle_id = dim.vehicle_id
where  dim.vehicle_id is null
union all
/*--existing data for comparizon*/
select *,
0 new_data from existing_data
)
,history_data as (
select
*,
/*-----------------------------------------------------------*/
coalesce(lead(transactioneffectivedate) over (partition by vehicle_uniqueid order by transactioneffectivedate), '3000-01-01') valid_todate,
/*-----------------------------------------------------------*/
lag(vin) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_vin,
lag(model) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_model,
lag(modelyr) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_modelyr,
lag(manufacturer) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_manufacturer
/*-----------------------------------------------------------*/
from data
)
select
vehicle_id,
policy_uniqueid,
transactioneffectivedate,
valid_todate,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer
from history_data
where new_data=1 and
(vin<>coalesce(lag_vin,'not_found') or
model<>coalesce(lag_model,'not_found') or
modelyr<>coalesce(lag_modelyr,'not_found') or
manufacturer<>coalesce(lag_manufacturer,'not_found'))
order by transactioneffectivedate;


/*---------------------------Insert New Data---------------------------*/

insert into {{ ref('dim_vehicle') }}
(
vehicle_id,
valid_fromdate,
valid_todate,
policy_uniqueid,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer,
loaddate
)
select
vehicle_id,
transactioneffectivedate valid_fromdate,
valid_todate,
policy_uniqueid,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer,
{{ loaddate() }}
from new_data;

/*----------------Adjusting valid_todate - valid_fromdate in a case of backdated transactions*/

with changed_data as (
select
vehicle_id,
vehicle_uniqueid,
valid_fromdate,
coalesce(lead(valid_fromdate) over (partition by vehicle_uniqueid order by valid_fromdate),'3000-01-01') valid_todate
from {{ ref('dim_vehicle') }} dim
where vehicle_uniqueid in (select stg.vehicle_uniqueid from new_data stg)
)
update {{ ref('dim_vehicle') }}
set valid_todate=changed_data.valid_todate
from changed_data
where changed_data.vehicle_id = {{ ref('dim_vehicle') }}.vehicle_id;



 {% endset %}




{% do run_query(insert_changed_data) %}


{% endmacro %}