/*------------------------------------------------------------------------------------------------------------*/
/*-------------------------------------------Simulation Extract from Source: Start ---------------------------*/
/*------------------------------------------------------------------------------------------------------------*/
--Extract from a source table
CREATE OR REPLACE TASK control_db.tasks.extract_policy_trn_from_source_task
    WAREHOUSE = compute_wh 
    SCHEDULE = '3 minute'
AS
DECLARE 
  LLTD_QUERY VARCHAR; 
  rs RESULTSET;
  UNLOAD_QUERY VARCHAR; 
  latest_transaction_date VARCHAR; 
BEGIN 

LLTD_QUERY := '  with latest_loaded_transactiondate as ' ||
' (SELECT transactiondate  ' ||
'  FROM mytest_db.POLICY_TRN_EXTTBL.orchestration_log /*Latest Loaded transactiondate*/ ' ||
'  WHERE LoadDate=(select max(t.loaddate) from mytest_db.POLICY_TRN_EXTTBL.orchestration_log t where t.endloaddate is not null)) ' ||
' SELECT min(h.transactiondate) transactiondate  ' ||
' FROM mytest_db.POLICY_TRN.orchestration_history h ' ||
' join latest_loaded_transactiondate ' ||
' on 1=1 ' ||
' WHERE h.transactiondate>latest_loaded_transactiondate.transactiondate ' ;

rs := (EXECUTE IMMEDIATE (:LLTD_QUERY));

FOR record IN rs DO

latest_transaction_date := record.transactiondate;
    
END FOR;

UNLOAD_QUERY := 
' copy into @control_db.external_stages.policy_trn_stage/staging/stg_pt/' || latest_transaction_date || '/policy_trn_' || TO_CHAR(current_timestamp(),'yyyymmddhh24miss') ||
'             from ( ' ||
'  with latest_loaded_transactiondate as ' ||
' (SELECT transactiondate  ' ||
'  FROM mytest_db.POLICY_TRN_EXTTBL.orchestration_log /*Latest Loaded transactiondate*/ ' ||
'  WHERE LoadDate=(select max(t.loaddate) from mytest_db.POLICY_TRN_EXTTBL.orchestration_log t where t.endloaddate is not null)) ' ||
' ,new_transactiondate as ( ' ||
' SELECT min(h.transactiondate) transactiondate  ' ||
' FROM mytest_db.POLICY_TRN.orchestration_history h ' ||
' join latest_loaded_transactiondate ' ||
' on 1=1 ' ||
' WHERE h.transactiondate>latest_loaded_transactiondate.transactiondate ' ||
' ) ' ||
' select stg.*  ' ||
' from mytest_db.policy_trn_source.stg_pt stg ' ||
' join latest_loaded_transactiondate ' ||
' on 1=1 ' ||
' join new_transactiondate ' ||
' on 1=1 ' ||
' where stg.transactiondate>latest_loaded_transactiondate.transactiondate ' ||
'   and stg.transactiondate<=new_transactiondate.transactiondate ' ||
'   ) ' ||
' file_format = (type = csv field_optionally_enclosed_by=''"'') ' ||
' OVERWRITE=TRUE; ';

EXECUTE IMMEDIATE (:UNLOAD_QUERY);


END; 

--Refresh external staging table
CREATE OR REPLACE TASK control_db.tasks.refresh_external_new_policy_stg_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.extract_policy_trn_from_source_task
AS
ALTER EXTERNAL TABLE MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT REFRESH;

--Start log
CREATE OR REPLACE TASK control_db.tasks.start_load_iceberg_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.refresh_external_new_policy_stg_task
AS
INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.ORCHESTRATION_LOG (loaddate, transactiondate)
select 
CURRENT_TIMESTAMP() loaddate,
max(cast(file_name_part as int)) transactiondate
from  MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT;

/*------------------------------------------------------------------------------------------------------------*/
/*--------------------------------------------LOAD DIMENSIONS-------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--DIM_POLICY
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_dim_policy_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.start_load_iceberg_task
AS
MERGE INTO mytest_db.POLICY_TRN_EXTTBL.dim_policy as tgt using
(
with most_latest_data as 
(
select 
policy_uniqueid,
max(transactionsequence) max_transactionsequence
from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg
group by policy_uniqueid
)
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
from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg
join most_latest_data
on stg.policy_uniqueid = most_latest_data.policy_uniqueid
and stg.transactionsequence = most_latest_data.max_transactionsequence
) data
  ON data.policy_uniqueid = tgt.policy_uniqueid   
WHEN NOT MATCHED THEN INSERT 
(
policy_id,
policy_uniqueid,
policynumber,
effectivedate,
expirationdate,
inceptiondate,
policystate,
carriercd,
companycd,
loaddate
) 
VALUES (
data.policy_id,
data.policy_uniqueid,
data.policynumber,
data.effectivedate,
data.expirationdate,
data.inceptiondate,
data.policystate,
data.carriercd,
data.companycd,
data.loaddate)
WHEN MATCHED THEN UPDATE 
SET 
tgt.policy_id = data.policy_id,
tgt.policy_uniqueid = data.policy_uniqueid,
tgt.policynumber = data.policynumber,
tgt.effectivedate = data.effectivedate,
tgt.expirationdate = data.expirationdate,
tgt.inceptiondate = data.inceptiondate,
tgt.policystate = data.policystate,
tgt.carriercd = data.carriercd,
tgt.companycd = data.companycd,
tgt.loaddate = data.loaddate;

/*------------------------------------------------------------------------------------------------------------*/

--DIM_COVERAGE
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_dim_coverage_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.start_load_iceberg_task
AS
/*no need to update because all columns is combined natural uniqueid*/
MERGE INTO mytest_db.POLICY_TRN_EXTTBL.dim_coverage as tgt using
(
select distinct
MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(
coveragecd,
subline,
asl), ':'))  coverage_id,
coalesce(stg.coveragecd,'~') as coveragecd,
coalesce(stg.subline,'~') as subline,
coalesce(stg.asl,'~') as asl,
CURRENT_TIMESTAMP() loaddate
from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg
) data
  ON data.coverage_id = tgt.coverage_id   
WHEN NOT MATCHED THEN INSERT
(
coverage_id,
coveragecd,
subline,
asl,
loaddate
)
values
(
data.coverage_id,
data.coveragecd,
data.subline,
data.asl,
data.loaddate
);

/*------------------------------------------------------------------------------------------------------------*/

--DIM_LIMIT
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_dim_limit_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.start_load_iceberg_task
AS
/*no need to update because all columns is combined natural uniqueid*/
MERGE INTO mytest_db.POLICY_TRN_EXTTBL.dim_limit as tgt using
(
select distinct
coalesce(stg.limit1,'~') as limit1,
coalesce(stg.limit2,'~') as limit2,
CURRENT_TIMESTAMP() loaddate
from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg
) data 
ON MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(data.limit1, data.limit2), ':'))  = MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(tgt.limit1, tgt.limit2), ':'))
WHEN NOT MATCHED THEN INSERT
(
limit_id,
limit1,
limit2,
loaddate
)
values
(
mytest_db.public.dim_limit_seq.NEXTVAL,
data.limit1,
data.limit2,
data.loaddate
);


/*------------------------------------------------------------------------------------------------------------*/

--DIM_DEDUCTIBLE
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_dim_deductible_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.start_load_iceberg_task
AS
/*no need to update because all columns is combined natural uniqueid*/
MERGE INTO mytest_db.POLICY_TRN_EXTTBL.dim_deductible as tgt using
(
with max_deductible as (select max(deductible_id) id from mytest_db.POLICY_TRN_EXTTBL.dim_deductible),
stg_data as (
select distinct
coalesce(stg.deductible1, 0) as deductible1,
coalesce(stg.deductible2, 0) as deductible2
from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg
)
, data as (
select
max_deductible.id + ROW_NUMBER() OVER(order by deductible1, deductible2) deductible_id,
deductible1,
deductible2,
CURRENT_TIMESTAMP() loaddate
from stg_data
join max_deductible on 1=1
)
select
data.deductible_id as deductible_id,
data.deductible1 as deductible1,
data.deductible2 as deductible2,
data.loaddate as loaddate
from data
) data 
ON MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(data.deductible1, data.deductible2), ':'))  = MD5(ARRAY_TO_STRING(ARRAY_CONSTRUCT(tgt.deductible1, tgt.deductible2), ':'))
WHEN NOT MATCHED THEN INSERT
(
deductible_id,
deductible1,
deductible2,
loaddate
)
values
(
data.deductible_id,
data.deductible1,
data.deductible2,
loaddate
);



/*------------------------------------------------------------------------------------------------------------*/

--DIM_DRIVER
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_dim_driver_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.start_load_iceberg_task
AS
call control_db.sps.load_iceberg_scd2_dim_driver();

/*------------------------------------------------------------------------------------------------------------*/

--DIM_VEHICLE
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_dim_vehicle_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.start_load_iceberg_task
AS
call control_db.sps.load_iceberg_scd2_dim_vehicle();


/*------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------------  FACT TABLES -------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--FACT_POLICYTTANSACTION
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_fact_policytransaction_task
  WAREHOUSE = COMPUTE_WH
  AFTER 
  control_db.tasks.load_iceberg_dim_policy_task,
  control_db.tasks.load_iceberg_dim_coverage_task,
  control_db.tasks.load_iceberg_dim_limit_task,
  control_db.tasks.load_iceberg_dim_deductible_task,
  control_db.tasks.load_iceberg_dim_driver_task,
  control_db.tasks.load_iceberg_dim_vehicle_task
AS
call control_db.sps.load_iceberg_fact_policytransaction();

/*------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------------  AGGREGATION FACT TABLE  -------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--DUMMMY_AGG_TABLE
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_agg_table_task
  WAREHOUSE = COMPUTE_WH
  AFTER control_db.tasks.load_iceberg_fact_policytransaction_task
AS
call control_db.sps.load_iceberg_agg_table(null);


/*------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------  END OF LOAD -------------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--log
CREATE OR REPLACE TASK control_db.tasks.end_load_iceberg_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_iceberg_agg_table_task
AS
update mytest_db.POLICY_TRN_EXTTBL.orchestration_log
set
 endloaddate = CURRENT_TIMESTAMP()
where endloaddate is null;

--notification email
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_complete_notification_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.end_load_iceberg_task
AS
call control_db.sps.load_iceberg_complete_notification();



--load testing PK
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_testing_PK_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_iceberg_complete_notification_task
AS
call control_db.sps.load_iceberg_testing_PK();

--load testing FK
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_testing_FK_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_iceberg_complete_notification_task
AS
call control_db.sps.load_iceberg_testing_FK();

--load testing
CREATE OR REPLACE TASK control_db.tasks.load_iceberg_testing_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_iceberg_complete_notification_task
AS
call control_db.sps.load_iceberg_testing();



alter task control_db.tasks.load_iceberg_testing_PK_task resume;
alter task control_db.tasks.load_iceberg_testing_FK_task resume;
alter task control_db.tasks.load_iceberg_testing_task resume;


alter task control_db.tasks.load_iceberg_complete_notification_task resume;
alter task control_db.tasks.end_load_iceberg_task resume;

alter task control_db.tasks.load_iceberg_dim_policy_task resume;
alter task control_db.tasks.load_iceberg_dim_coverage_task resume;
alter task control_db.tasks.load_iceberg_dim_limit_task resume;
alter task control_db.tasks.load_iceberg_dim_deductible_task resume;
alter task control_db.tasks.load_iceberg_dim_driver_task resume;
alter task control_db.tasks.load_iceberg_dim_vehicle_task resume;


alter task control_db.tasks.load_iceberg_agg_table_task resume;
alter task control_db.tasks.load_iceberg_fact_policytransaction_task resume;



alter task control_db.tasks.start_load_iceberg_task resume;
alter task control_db.tasks.refresh_external_new_policy_stg_task resume;
alter task control_db.tasks.extract_policy_trn_from_source_task resume;


--

alter task control_db.tasks.extract_policy_trn_from_source_task suspend;
alter task control_db.tasks.refresh_external_new_policy_stg_task suspend;
alter task control_db.tasks.start_load_iceberg_task suspend;


alter task control_db.tasks.load_iceberg_agg_table_task suspend;
alter task control_db.tasks.load_iceberg_fact_policytransaction_task suspend;


alter task control_db.tasks.load_iceberg_dim_policy_task suspend;
alter task control_db.tasks.load_iceberg_dim_coverage_task suspend;
alter task control_db.tasks.load_iceberg_dim_limit_task suspend;
alter task control_db.tasks.load_iceberg_dim_deductible_task suspend;
alter task control_db.tasks.load_iceberg_dim_driver_task suspend;
alter task control_db.tasks.load_iceberg_dim_vehicle_task suspend;


alter task control_db.tasks.end_load_iceberg_task suspend;
alter task control_db.tasks.load_iceberg_complete_notification_task suspend;

alter task control_db.tasks.load_iceberg_testing_PK_task suspend;
alter task control_db.tasks.load_iceberg_testing_FK_task suspend;
alter task control_db.tasks.load_iceberg_testing_task suspend;



USE SCHEMA control_db.tasks;
show tasks;
select * from table(result_scan(last_query_id()))
where "name" like '%ICEBERG%';



select name, query_text, error_message, scheduled_time  from
table(information_schema.TASK_HISTORY(
  --  SCHEDULED_TIME_RANGE_START=>TO_TIMESTAMP_LTZ('2022-05-26 10:00:00.00 -0700'),
  --  SCHEDULED_TIME_RANGE_END=>TO_TIMESTAMP_LTZ('2022-05-26 11:00:00.00 -0700'),
  --  RESULT_LIMIT => 10,
  --  TASK_NAME => 'DEMO_TASK',
    ERROR_ONLY => TRUE)) 
    order by scheduled_time desc;

    

select * from mytest_db.POLICY_TRN_EXTTBL.orchestration_log;

select count(*) from mytest_db.POLICY_TRN_EXTTBL.dim_policy;

select * from mytest_db.POLICY_TRN_EXTTBL.dim_deductible;

