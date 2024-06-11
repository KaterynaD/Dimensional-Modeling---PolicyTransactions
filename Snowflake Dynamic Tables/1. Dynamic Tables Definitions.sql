USE DATABASE MYTEST_DB;

CREATE SCHEMA POLICY_TRN_DYNTBL;

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_POLICY
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = AUTO
  INITIALIZE = ON_SCHEDULE
  AS
with most_latest_data as 
(
select 
policy_uniqueid,
max(transactionsequence) max_transactionsequence
from mytest_db.policy_trn_staging.stg_pt stg
group by policy_uniqueid
)
,data as (
select
0 policy_id,
0 policy_uniqueid,
'Unknown' policynumber,
'1900-01-01' effectivedate,
'2900-01-01' expirationdate,
'1900-01-01' inceptiondate,
'~' policystate,
'~' carriercd,
'~' companycd,
CURRENT_TIMESTAMP() loaddate
union all
select distinct
coalesce(stg.policy_uniqueid,0) as policy_id,
coalesce(stg.policy_uniqueid,0) as policy_uniqueid,
coalesce(stg.policynumber,'~') as policynumber,
coalesce(stg.effectivedate,'1900-01-01') as effectivedate,
coalesce(stg.expirationdate,'1900-01-01') as expirationdate,
coalesce(stg.inceptiondate,'1900-01-01') as inceptiondate,
coalesce(stg.policystate,'~') as policystate,
coalesce(stg.carriercd,'~') as carriercd,
coalesce(stg.companycd,'~') as companycd,
CURRENT_TIMESTAMP()  loaddate
from mytest_db.policy_trn_staging.stg_pt stg
join most_latest_data
on stg.policy_uniqueid = most_latest_data.policy_uniqueid
and stg.transactionsequence = most_latest_data.max_transactionsequence
)
select
data.policy_id::integer as policy_id,
data.policy_uniqueid::integer as policy_uniqueid,
data.policynumber::varchar(20) as policynumber,
data.effectivedate::date as effectivedate,
data.expirationdate::date as expirationdate,
data.inceptiondate::date as inceptiondate,
data.policystate::varchar(2) as policystate,
data.carriercd::varchar(10) as carriercd,
data.companycd::varchar(10) as companycd,
data.loaddate::timestamp without time zone as loaddate
from data;


ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_POLICY add CONSTRAINT pk_policy  PRIMARY KEY ( POLICY_ID );

CREATE OR REPLACE DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_COVERAGE
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = AUTO
  INITIALIZE = ON_SCHEDULE
  AS
with data as (
select
'Unknown' coverage_id,
'~' coveragecd,
'~' subline,
'~' asl,
CURRENT_TIMESTAMP() loaddate
union all
select distinct
MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(
coveragecd,
subline,
asl), ':'))  coverage_id,
coalesce(stg.coveragecd,'~') as coveragecd,
coalesce(stg.subline,'~') as subline,
coalesce(stg.asl,'~') as asl,
CURRENT_TIMESTAMP() loaddate
from mytest_db.policy_trn_staging.stg_pt stg
)
select
data.coverage_id::varchar(100) as coverage_id,
data.coveragecd::varchar(20) as coveragecd,
data.subline::varchar(5) as subline,
data.asl::varchar(5) as asl,
data.loaddate::timestamp without time zone as loaddate
from data;

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_COVERAGE add CONSTRAINT pk_COVERAGE  PRIMARY KEY ( COVERAGE_ID );


CREATE OR REPLACE DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DEDUCTIBLE
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = AUTO
  INITIALIZE = ON_SCHEDULE
  AS
with  data as (
select
'Unknown' deductible_id,
0.0 deductible1,
0.0 deductible2,
CURRENT_TIMESTAMP() loaddate
union all
select distinct
MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(
 stg.deductible1,
 stg.deductible2), ':')) as deductible_id,
coalesce(stg.deductible1, 0) as deductible1,
coalesce(stg.deductible2, 0) as deductible2,
CURRENT_TIMESTAMP() loaddate
from mytest_db.policy_trn_staging.stg_pt stg
where stg.deductible1>0
)
select
data.deductible_id::varchar(100) as deductible_id,
data.deductible1::numeric as deductible1,
data.deductible2::numeric as deductible2,
data.loaddate::timestamp without time zone as loaddate
from data;


ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DEDUCTIBLE add CONSTRAINT pk_DEDUCTIBLE  PRIMARY KEY ( DEDUCTIBLE_ID );

CREATE OR REPLACE DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_LIMIT
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = AUTO
  INITIALIZE = ON_SCHEDULE
  AS
with  data as (  
select
'Unknown' limit_id,
'~' limit1,
'~' limit2,
CURRENT_TIMESTAMP() loaddate
union all
select distinct
MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(
 coalesce(stg.limit1,'~'),
 coalesce(stg.limit2,'~')), ':')) as limit_id,
coalesce(stg.limit1,'~') as limit1,
coalesce(stg.limit2,'~') as limit2,
CURRENT_TIMESTAMP() loaddate
from mytest_db.policy_trn_staging.stg_pt stg
where not (coalesce(stg.limit1,'~')='~' and coalesce(stg.limit2,'~')='~')
)
select
data.limit_id::varchar(100) as limit_id,
data.limit1::varchar(100) as limit1,
data.limit2::varchar(100) as limit2,
data.loaddate::timestamp without time zone as loaddate
from data;


ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_LIMIT add CONSTRAINT pk_LIMIT  PRIMARY KEY ( LIMIT_ID );


create or replace view MYTEST_DB.POLICY_TRN_DYNTBL.STG_DRIVER(
	DRIVER_ID,
	TRANSACTIONEFFECTIVEDATE,
	DRIVER_UNIQUEID,
	GENDERCD,
	BIRTHDATE,
	MARITALSTATUSCD,
	POINTSCHARGED
) COMMENT='SCD2 Driver staging view to provide logic in load_scd2'
 as (  
with data as (
/*There is a small amount of data issues in the staging data related to different attributes for the same uniqueid and transactioneffective date.
  No way to automatically select a proper record. The issue is in the source system processing XML data.
  Any record can be valid. In this case I select just max attribute value for simplicity.
*/        
select 
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) , '')
         || '|' || coalesce(cast(stg.drv_effectivedate::timestamp without time zone as varchar ), '')
        )  driver_id,
stg.drv_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) driver_uniqueid,
max(stg.gendercd) as gendercd,
max(to_char(stg.birthdate,'yyyy-mm-dd')) as birthdate,
max(stg.maritalstatuscd) as maritalstatuscd,
max(cast(stg.pointschargedterm as varchar)) as  pointscharged
from mytest_db.policy_trn_staging.stg_pt stg
group by 
stg.drv_effectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) 
)
select
data.driver_id::varchar(100) as driver_id,
data.transactioneffectivedate::timestamp without time zone as transactioneffectivedate,
data.driver_uniqueid::varchar(100) as driver_uniqueid,
data.gendercd::varchar(10) as gendercd,
data.birthdate::varchar(10) as birthdate,
data.maritalstatuscd::varchar(10) as maritalstatuscd,
data.pointscharged::varchar(3) as pointscharged
from data
  );



CREATE OR REPLACE DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = FULL
  INITIALIZE = ON_SCHEDULE
  AS
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
from mytest_db.POLICY_TRN_DYNTBL.stg_driver stg
order by transactioneffectivedate
)
,history_data as (
select
*,
/*-----------------------------------------------------------*/
/*-----------------------------------------------------------*/
lag(gendercd) over (partition by driver_uniqueid order by transactioneffectivedate) lag_gendercd,
lag(birthdate) over (partition by driver_uniqueid order by transactioneffectivedate) lag_birthdate,
lag(maritalstatuscd) over (partition by driver_uniqueid order by transactioneffectivedate) lag_maritalstatuscd,
lag(pointscharged) over (partition by driver_uniqueid order by transactioneffectivedate) lag_pointscharged
/*-----------------------------------------------------------*/
from stg_data
)
,data as (
select
driver_id,
transactioneffectivedate as valid_fromdate,
coalesce(lead(transactioneffectivedate) over (partition by driver_uniqueid order by transactioneffectivedate), '3000-12-31'::timestamp without time zone) valid_todate,
driver_uniqueid,
gendercd,
birthdate,
maritalstatuscd,
pointscharged,
CURRENT_TIMESTAMP() loaddate
from history_data
where 
(
gendercd<>coalesce(lag_gendercd,'not_found') or
birthdate<>coalesce(lag_birthdate,'not_found') or
maritalstatuscd<>coalesce(lag_maritalstatuscd,'not_found') or
pointscharged<>coalesce(lag_pointscharged,'not_found')
)
/*for simplicity assuming all columns are varchar, otherwise need to check data type somehow and use appropriate default*/
)
select
data.driver_id::varchar(100) as driver_id,
data.valid_fromdate::timestamp without time zone as valid_fromdate,
data.valid_todate::timestamp without time zone as valid_todate,
data.driver_uniqueid::varchar(100) as driver_uniqueid,
data.gendercd::varchar(10) as gendercd,
data.birthdate::varchar(10) as birthdate,
data.maritalstatuscd::varchar(10) as maritalstatuscd,
data.pointscharged::varchar(3) as pointscharged,
data.loaddate::timestamp without time zone as loaddate
from data
order by driver_uniqueid, valid_fromdate;


ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER add CONSTRAINT pk_DRIVER  PRIMARY KEY ( DRIVER_ID );

create or replace view MYTEST_DB.POLICY_TRN_DYNTBL.STG_VEHICLE(
	VEHICLE_ID,
	TRANSACTIONEFFECTIVEDATE,
	VEHICLE_UNIQUEID,
	VIN,
	MODEL,
	MODELYR,
	MANUFACTURER,
	ESTIMATEDANNUALDISTANCE
) COMMENT='SCD2 Vehicle staging view to provide logic in load_scd2'
 as (   
with data as (
/*There is a small amount of data issues in the staging data related to different attributes for the same uniqueid and transactioneffective date.
  No way to automatically select a proper record. The issue is in the source system processing XML data.
  Any record can be valid. In this case I select just max attribute value for simplicity.
*/
select 
md5(coalesce(concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) , '')
         || '|' || coalesce(cast(stg.veh_effectivedate::timestamp without time zone as varchar ), '')
        )  vehicle_id,
stg.veh_effectivedate transactioneffectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) vehicle_uniqueid,
max(stg.vin) as  vin,
max(stg.model) as model,
max(stg.modelyr) as modelyr,
max(stg.manufacturer) as manufacturer,
max(stg.estimatedannualdistance) as estimatedannualdistance
from mytest_db.policy_trn_staging.stg_pt stg
group by
stg.veh_effectivedate,
concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) )
)
select
data.vehicle_id::varchar(100) as vehicle_id,
data.transactioneffectivedate::timestamp without time zone as transactioneffectivedate,
data.vehicle_uniqueid::varchar(100) as vehicle_uniqueid,
data.vin::varchar(20) as vin,
data.model::varchar(100) as model,
data.modelyr::varchar(100) as modelyr,
data.manufacturer::varchar(100) as manufacturer,
data.estimatedannualdistance::varchar(100) as estimatedannualdistance
from data
  );


CREATE OR REPLACE DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = FULL
  INITIALIZE = ON_SCHEDULE
  AS
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
from mytest_db.POLICY_TRN_DYNTBL.stg_vehicle stg
)
,history_data as (
select
*,
/*-----------------------------------------------------------*/
/*-----------------------------------------------------------*/
lag(vin) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_vin,
lag(model) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_model,
lag(modelyr) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_modelyr,
lag(manufacturer) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_manufacturer,
lag(estimatedannualdistance) over (partition by vehicle_uniqueid order by transactioneffectivedate) lag_estimatedannualdistance
/*-----------------------------------------------------------*/
from stg_data
)
,data as (
select
vehicle_id,
transactioneffectivedate valid_fromdate,
coalesce(lead(transactioneffectivedate) over (partition by vehicle_uniqueid order by transactioneffectivedate), '3000-12-31'::timestamp without time zone) valid_todate,
vehicle_uniqueid,
vin,
model,
modelyr,
manufacturer,
estimatedannualdistance,
CURRENT_TIMESTAMP() loaddate
from history_data
where 
(
vin<>coalesce(lag_vin,'not_found') or
model<>coalesce(lag_model,'not_found') or
modelyr<>coalesce(lag_modelyr,'not_found') or
manufacturer<>coalesce(lag_manufacturer,'not_found') or
estimatedannualdistance<>coalesce(lag_estimatedannualdistance,'not_found')
)
/*for simplicity assuming all columns are varchar, otherwise need to check data type somehow and use appropriate default*/
)
select
vehicle_id::varchar(100) as vehicle_id,
valid_fromdate::timestamp without time zone as valid_fromdate,
valid_todate::timestamp without time zone as valid_todate,
vehicle_uniqueid::varchar(100) as vehicle_uniqueid,
vin::varchar(20) as vin,
model::varchar(100) as model,
modelyr::varchar(100) as modelyr,
manufacturer::varchar(100) as manufacturer,
estimatedannualdistance::varchar(100) as estimatedannualdistance,
data.loaddate::timestamp without time zone as loaddate
from data
order by vehicle_uniqueid, valid_fromdate;

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE add CONSTRAINT pk_VEHICLE  PRIMARY KEY ( VEHICLE_ID );


CREATE OR REPLACE DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = compute_wh
  REFRESH_MODE = AUTO
  INITIALIZE = ON_SCHEDULE
  AS
with data as (
select 
/*-----------------------------------------------------------------------*/
md5(transactiondate) as policytransaction_id,
/*-----------------------------------------------------------------------*/        
stg.transactiondate,
stg.accountingdate,
/*-----------------------------------------------------------------------*/
coalesce(dim_transaction.transaction_id,0) as transaction_id,
stg.transactioneffectivedate,
stg.transactionsequence,
/*-----------------------------------------------------------------------*/
coalesce(dim_policy.policy_id, 0) as policy_id,
stg.veh_effectivedate,
coalesce(dim_vehicle.vehicle_id, 'Unknown') as vehicle_id,
coalesce(dim_driver.driver_id, 'Unknown') as driver_id,
coalesce(dim_coverage.coverage_id, 'Unknown') as coverage_id,
coalesce(dim_limit.limit_id, 'Unknown') as limit_id,
coalesce(dim_deductible.deductible_id, 'Unknown') as deductible_id,
/*-----------------------------------------------------------------------*/
stg.amount
/*-----------------------------------------------------------------------*/
/*=======================================================================*/
from mytest_db.policy_trn_staging.stg_pt stg
--
left outer join mytest_db.POLICY_TRN.dim_transaction dim_transaction
on stg.transactioncd=dim_transaction.transactioncd
--
left outer join mytest_db.POLICY_TRN_DYNTBL.dim_policy dim_policy
on stg.policy_uniqueid=dim_policy.policy_uniqueid
--
left outer join mytest_db.POLICY_TRN_DYNTBL.dim_coverage dim_coverage
on stg.coveragecd=dim_coverage.coveragecd
and stg.subline=dim_coverage.subline
and stg.asl=dim_coverage.asl
--
left outer join mytest_db.POLICY_TRN_DYNTBL.dim_limit dim_limit
on stg.limit1=dim_limit.limit1
and stg.limit2=dim_limit.limit2
--
left outer join mytest_db.POLICY_TRN_DYNTBL.dim_deductible dim_deductible
on stg.deductible1=dim_deductible.deductible1
and stg.deductible2=dim_deductible.deductible2
--
left outer join mytest_db.POLICY_TRN_DYNTBL.dim_driver dim_driver
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) = dim_driver.driver_uniqueid
and (stg.drv_effectivedate >= dim_driver.valid_fromdate 
and stg.drv_effectivedate < dim_driver.valid_todate)

--
left outer join mytest_db.POLICY_TRN_DYNTBL.dim_vehicle dim_vehicle
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd1 as varchar) ) = dim_vehicle.vehicle_uniqueid
and (stg.veh_effectivedate >= dim_vehicle.valid_fromdate 
and stg.veh_effectivedate < dim_vehicle.valid_todate)
/*=======================================================================*/
)
select
/*-----------------------------------------------------------------------*/
data.policytransaction_id as policytransaction_id,
/*-----------------------------------------------------------------------*/        
data.transactiondate as transactiondate,
data.accountingdate as accountingdate,
/*-----------------------------------------------------------------------*/
data.transaction_id as transaction_id,
data.transactioneffectivedate as transactioneffectivedate,
data.transactionsequence as transactionsequence,
/*-----------------------------------------------------------------------*/
data.policy_id as policy_id,
data.veh_effectivedate,
data.vehicle_id as vehicle_id,
data.driver_id as driver_id,
data.coverage_id as coverage_id,
data.limit_id as limit_id,
data.deductible_id as deductible_id,
/*-----------------------------------------------------------------------*/
data.amount as amount,
CURRENT_TIMESTAMP() loaddate
from data;


ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add 
CONSTRAINT FK_TRANSACTION_IS_TYPE FOREIGN KEY ( TRANSACTION_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_TRANSACTION ( TRANSACTION_ID ) RELY   COMMENT  'Transaction`s type. A type can have zero or many transactions.';

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add 
    CONSTRAINT FK_TRANSACTION_FOR_POLICY FOREIGN KEY ( POLICY_ID ) REFERENCES MYTEST_DB.POLICY_TRN_DYNTBL.DIM_POLICY ( POLICY_ID ) RELY   COMMENT  'Transaction`s based policy. A policy can have one or many transactions.';

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add     
    CONSTRAINT FK_TRANSACTION_FOR_VEHICLE FOREIGN KEY ( VEHICLE_ID ) REFERENCES MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE ( VEHICLE_ID ) RELY   COMMENT  'Transaction`s based vehicle. A vehicle can have one or many transactions.';

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add     
    CONSTRAINT FK_TRANSACTION_FOR_DRIVER FOREIGN KEY ( DRIVER_ID ) REFERENCES MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER ( DRIVER_ID ) RELY   COMMENT  'Transaction`s based driver. A driver can have zero or many transactions. Drivers without assigned vehicles may not have transactions';

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add     
    CONSTRAINT FK_TRANSACTION_SETs_COVERAGE FOREIGN KEY ( COVERAGE_ID ) REFERENCES MYTEST_DB.POLICY_TRN_DYNTBL.DIM_COVERAGE ( COVERAGE_ID ) RELY   COMMENT  'A coverage is set by a transaction. A coverage always has at least one transaction';

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add     
    CONSTRAINT FK_TRANSACTION_SETs_LIMIT FOREIGN KEY ( LIMIT_ID ) REFERENCES MYTEST_DB.POLICY_TRN_DYNTBL.DIM_LIMIT ( LIMIT_ID ) RELY   COMMENT  'A limit is set by a transaction. A limit always has at least one transaction';

ALTER table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION add     
    CONSTRAINT FK_TRANSACTION_SETs_DEDUCTIBLE FOREIGN KEY ( DEDUCTIBLE_ID ) REFERENCES MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DEDUCTIBLE ( DEDUCTIBLE_ID ) RELY   COMMENT  'A deductible is set by a transaction. A deductible always has at least one transaction' ; 


--Test
with staging as 
(
select count(*) cnt ,
sum(stg.amount) sum_amount
from mytest_db.policy_trn_staging.stg_pt stg
)
, fact as 
(
select count(*) cnt  ,
sum(amount) sum_amount
from mytest_db.POLICY_TRN_DYNTBL.fact_policytransaction 
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
staging.sum_amount <> fact.sum_amount;



select count(*) from MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE
--25,153

select  count(*) from MYTEST_DB.POLICY_TRN.DIM_VEHICLE
--25,184

select *
from MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE
where VEHICLE_uniqueid='1685079_1'
order by valid_todate


select *
from MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER
where driver_uniqueid='1746395_2'
order by valid_todate;



ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_POLICY REFRESH;
ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_COVERAGE REFRESH;
ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DEDUCTIBLE REFRESH;
ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_LIMIT REFRESH;
ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER REFRESH;
ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE REFRESH;
ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION REFRESH;


drop table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_POLICY;

drop table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_COVERAGE;
drop table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DEDUCTIBLE;
drop table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_LIMIT;

drop table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER;
drop table MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE;

drop table MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION;


select * from MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION;

SELECT
  name,
  state,
  state_code,
  state_message,
  query_id,
  data_timestamp,
  refresh_start_time,
  refresh_end_time
FROM
  TABLE (
    INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY (
      NAME_PREFIX => 'MYTEST_DB.POLICY_TRN_DYNTBL.' --, ERROR_ONLY => TRUE
    )
  )
  order by refresh_start_time desc;


  truncate table mytest_db.policy_trn_staging.stg_pt;

  insert into mytest_db.policy_trn_staging.stg_pt
  select *
  from mytest_db.policy_trn_source.stg_pt;

  