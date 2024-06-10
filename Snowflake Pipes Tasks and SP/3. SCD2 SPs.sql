
CREATE OR REPLACE PROCEDURE control_db.sps.load_scd2_dim_driver()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
  num_updated integer default 0;
  num_inserted integer default 0;
BEGIN

 /*assuming all business-info columns in the table should track history
   it's possible to have business-info columns which should be just updated as in SCD1
   but more aguments and updates is needed in the code
 */

/*-------------------Forward Dated Changes-------------------------*/

/*only new changed data*/

create or replace temporary table new_data as
with stg_data as (
/*--new data in stage can have changes for the same uniqueid - historical load as an example --*/
select distinct
driver_id,
transactioneffectivedate,
driver_uniqueid,
stg.gendercd,
stg.birthdate,
stg.maritalstatuscd,
stg.pointscharged
from mytest_db.policy_trn_staging.stg_driver stg
)
/*-- we need existing data from dim to compare with new add/not add with/without changes --*/
, existing_data as (
select
dim.driver_id,
dim.valid_fromdate transactioneffectivedate,
dim.driver_uniqueid,
dim.gendercd,
dim.birthdate,
dim.maritalstatuscd,
dim.pointscharged
from  mytest_db.policy_trn.dim_driver dim
where dim.driver_uniqueid in (select stg.driver_uniqueid from stg_data stg)
)
,data as (
/*---new data ---*/
select stg.* ,
1 new_data
from stg_data stg 
left outer join existing_data dim 
on stg.driver_id = dim.driver_id
where  dim.driver_id is null
union all
/*--existing data for comparizon*/
select *,
0 new_data from existing_data
)
,history_data as (
select
*,
/*-----------------------------------------------------------*/
coalesce(lead(transactioneffectivedate) over (partition by driver_uniqueid order by transactioneffectivedate), '3000-12-31'::timestamp without time zone) valid_todate,
/*-----------------------------------------------------------*/
lag(gendercd) over (partition by driver_uniqueid order by transactioneffectivedate) lag_gendercd,
lag(birthdate) over (partition by driver_uniqueid order by transactioneffectivedate) lag_birthdate,
lag(maritalstatuscd) over (partition by driver_uniqueid order by transactioneffectivedate) lag_maritalstatuscd,
lag(pointscharged) over (partition by driver_uniqueid order by transactioneffectivedate) lag_pointscharged
/*-----------------------------------------------------------*/
from data
)
select
driver_id,
transactioneffectivedate,
valid_todate,
driver_uniqueid,
gendercd,
birthdate,
maritalstatuscd,
pointscharged,
1 dummy
from history_data
where new_data=1 and
(
gendercd<>coalesce(lag_gendercd,'not_found') or
birthdate<>coalesce(lag_birthdate,'not_found') or
maritalstatuscd<>coalesce(lag_maritalstatuscd,'not_found') or
pointscharged<>coalesce(lag_pointscharged,'not_found')
)
/*for simplicity assuming all columns are varchar, otherwise need to check data type somehow and use appropriate default*/
order by transactioneffectivedate;


/*---------------------------Insert New Data---------------------------*/

insert into mytest_db.policy_trn.dim_driver
(
driver_id,
valid_fromdate,
valid_todate,
driver_uniqueid,
gendercd,
birthdate,
maritalstatuscd,
pointscharged,
loaddate
)
select
driver_id,
transactioneffectivedate valid_fromdate,
valid_todate,
driver_uniqueid,
gendercd,
birthdate,
maritalstatuscd,
pointscharged,
CURRENT_TIMESTAMP() loaddate
from new_data;

num_inserted := SQLROWCOUNT;

/*----------------Adjusting valid_todate - valid_fromdate in a case of backdated transactions*/
update mytest_db.policy_trn.dim_driver t
set t.valid_todate=ctes.valid_todate
from (
with changed_data as (
select
driver_id,
driver_uniqueid,
valid_fromdate,
coalesce(lead(valid_fromdate) over (partition by driver_uniqueid order by valid_fromdate),'3000-12-31'::timestamp without time zone) valid_todate
from mytest_db.policy_trn.dim_driver dim
where driver_uniqueid in (select stg.driver_uniqueid from new_data stg)
)
select * from changed_data
) as ctes
where ctes.driver_id = t.driver_id;

num_updated := SQLROWCOUNT;

RETURN 'Updated: ' ||  num_updated || '. Inserted: ' || num_inserted || '.';

END;






CREATE OR REPLACE PROCEDURE control_db.sps.load_scd2_dim_vehicle()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
  num_updated integer default 0;
  num_inserted integer default 0;
BEGIN

 /*assuming all business-info columns in the table should track history
   it's possible to have business-info columns which should be just updated as in SCD1
   but more aguments and updates is needed in the code
 */

/*-------------------Forward Dated Changes-------------------------*/

/*only new changed data*/

create or replace temporary table new_data as
with stg_data as (
/*--new data in stage can have changes for the same uniqueid - historical load as an example --*/
select distinct
vehicle_id,
transactioneffectivedate,
vehicle_uniqueid,
stg.vin,
stg.model,
stg.modelyr,
stg.manufacturer,
stg.estimatedannualdistance
from mytest_db.policy_trn_staging.stg_vehicle stg
)
/*-- we need existing data from dim to compare with new add/not add with/without changes --*/
, existing_data as (
select
dim.vehicle_id,
dim.valid_fromdate transactioneffectivedate,
dim.vehicle_uniqueid,
dim.vin,
dim.model,
dim.modelyr,
dim.manufacturer,
dim.estimatedannualdistance
from  mytest_db.policy_trn.dim_vehicle dim
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
coalesce(lead(transactioneffectivedate) over (partition by vehicle_uniqueid order by transactioneffectivedate), '3000-12-31'::timestamp without time zone) valid_todate,
/*-----------------------------------------------------------*/
lag(vin) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_vin,
lag(model) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_model,
lag(modelyr) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_modelyr,
lag(manufacturer) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_manufacturer,
lag(estimatedannualdistance) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_estimatedannualdistance
/*-----------------------------------------------------------*/
from data
)
select
vehicle_id,
transactioneffectivedate,
valid_todate,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer,
estimatedannualdistance,
1 dummy
from history_data
where new_data=1 and
(
vin<>coalesce(lag_vin,'not_found') or
model<>coalesce(lag_model,'not_found') or
modelyr<>coalesce(lag_modelyr,'not_found') or
manufacturer<>coalesce(lag_manufacturer,'not_found') or
estimatedannualdistance<>coalesce(lag_estimatedannualdistance,'not_found')
)
/*for simplicity assuming all columns are varchar, otherwise need to check data type somehow and use appropriate default*/
order by transactioneffectivedate;


/*---------------------------Insert New Data---------------------------*/

insert into mytest_db.policy_trn.dim_vehicle
(
vehicle_id,
valid_fromdate,
valid_todate,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer,
estimatedannualdistance,
loaddate
)
select
vehicle_id,
transactioneffectivedate valid_fromdate,
valid_todate,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer,
estimatedannualdistance,
CURRENT_TIMESTAMP() loaddate
from new_data;

num_inserted := SQLROWCOUNT;

/*----------------Adjusting valid_todate - valid_fromdate in a case of backdated transactions*/
update mytest_db.policy_trn.dim_vehicle t
set t.valid_todate=ctes.valid_todate
from (
with changed_data as (
select
vehicle_id,
vehicle_uniqueid,
valid_fromdate,
coalesce(lead(valid_fromdate) over (partition by vehicle_uniqueid order by valid_fromdate),'3000-12-31'::timestamp without time zone) valid_todate
from mytest_db.policy_trn.dim_vehicle dim
where vehicle_uniqueid in (select stg.vehicle_uniqueid from new_data stg)
)
select * from changed_data
) as ctes
where ctes.vehicle_id = t.vehicle_id;

num_updated := SQLROWCOUNT;

RETURN 'Updated: ' ||  num_updated || '. Inserted: ' || num_inserted || '.';

END;











