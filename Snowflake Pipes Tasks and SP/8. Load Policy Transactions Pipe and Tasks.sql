-- Create an append-only stream on the staging table.
create or replace  stream mytest_db.policy_trn_staging.stg_pt_append_only_stream on table mytest_db.policy_trn_staging.stg_pt append_only=true;

-- Load into staging table
create or replace pipe control_db.pipes.policy_trn_new_data_pipe auto_ingest=true as
copy into mytest_db.policy_trn_staging.stg_pt 
from @control_db.external_stages.policy_trn_stage/new_policy_trn
file_format = (FORMAT_NAME='control_db.file_formats.csv_format');

ALTER PIPE control_db.pipes.policy_trn_new_data_pipe SET PIPE_EXECUTION_PAUSED = FALSE;



CREATE OR REPLACE PROCEDURE control_db.sps.new_data_only()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
  num_inserted integer default 0;
BEGIN
 truncate table mytest_db.policy_trn_staging.stg_pt_new_data;
 
 insert into mytest_db.policy_trn_staging.stg_pt_new_data
 select * exclude (METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID)
 from mytest_db.policy_trn_staging.stg_pt_append_only_stream;

 num_inserted := num_inserted + SQLROWCOUNT;
    
    
 RETURN 'Inserted: ' || num_inserted || '.';
  
END;

CREATE OR REPLACE TASK control_db.tasks.load_new_policy_trn_task
    WAREHOUSE = compute_wh 
    SCHEDULE = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('mytest_db.policy_trn_staging.stg_pt_append_only_stream') 
AS
call control_db.sps.new_data_only();

/*------------------------------------------------------------------------------------------------------------*/
/*--------------------------------------------LOAD DIMENSIONS-------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--DIM_POLICY
CREATE OR REPLACE TASK control_db.tasks.load_dim_policy_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_new_policy_trn_task
AS
MERGE INTO mytest_db.policy_trn.dim_policy as tgt using
(
with most_latest_data as 
(
select 
policy_uniqueid,
max(transactionsequence) max_transactionsequence
from mytest_db.policy_trn_staging.stg_pt_new_data stg
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
from mytest_db.policy_trn_staging.stg_pt_new_data stg
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
CREATE OR REPLACE TASK control_db.tasks.load_dim_coverage_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_new_policy_trn_task
AS
/*no need to update because all columns is combined natural uniqueid*/
MERGE INTO mytest_db.policy_trn.dim_coverage as tgt using
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
from mytest_db.policy_trn_staging.stg_pt_new_data stg
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
CREATE OR REPLACE TASK control_db.tasks.load_dim_limit_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_new_policy_trn_task
AS
/*no need to update because all columns is combined natural uniqueid*/
MERGE INTO mytest_db.policy_trn.dim_limit as tgt using
(
select distinct
coalesce(stg.limit1,'~') as limit1,
coalesce(stg.limit2,'~') as limit2,
CURRENT_TIMESTAMP() loaddate
from mytest_db.policy_trn_staging.stg_pt_new_data stg
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
mytest_db.policy_trn.dim_limit_seq.NEXTVAL,
data.limit1,
data.limit2,
data.loaddate
);


/*------------------------------------------------------------------------------------------------------------*/

--DIM_DEDUCTIBLE
CREATE OR REPLACE TASK control_db.tasks.load_dim_deductible_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_new_policy_trn_task
AS
/*no need to update because all columns is combined natural uniqueid*/
MERGE INTO mytest_db.policy_trn.dim_deductible as tgt using
(
with max_deductible as (select max(deductible_id) id from mytest_db.policy_trn.dim_deductible),
stg_data as (
select distinct
coalesce(stg.deductible1, 0) as deductible1,
coalesce(stg.deductible2, 0) as deductible2
from mytest_db.policy_trn_staging.stg_pt_new_data stg
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
CREATE OR REPLACE TASK control_db.tasks.load_dim_driver_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_new_policy_trn_task
AS
call control_db.sps.load_scd2_dim_driver();

/*------------------------------------------------------------------------------------------------------------*/

--DIM_VEHICLE
CREATE OR REPLACE TASK control_db.tasks.load_dim_vehicle_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_new_policy_trn_task
AS
call control_db.sps.load_scd2_dim_vehicle();


/*------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------------  FACT TABLES -------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--FACT_POLICYTTANSACTION
CREATE TASK control_db.tasks.load_fact_policytransaction_task
  WAREHOUSE = COMPUTE_WH
  AFTER 
  control_db.tasks.load_dim_policy_task,
  control_db.tasks.load_dim_coverage_task,
  control_db.tasks.load_dim_limit_task,
  control_db.tasks.load_dim_deductible_task,
  control_db.tasks.load_dim_driver_task,
  control_db.tasks.load_dim_vehicle_task
AS
call control_db.sps.load_fact_policytransaction();

/*------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------------  AGGREGATION FACT TABLE  -------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/

--DUMMMY_AGG_TABLE
CREATE TASK control_db.tasks.load_agg_table_task
  WAREHOUSE = COMPUTE_WH
  AFTER control_db.tasks.load_fact_policytransaction_task
AS
call control_db.sps.load_agg_table(null);


/*------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------  END OF LOAD -------------------------------------------------------*/
/*------------------------------------------------------------------------------------------------------------*/


--Purge data from S3
CREATE OR REPLACE TASK control_db.tasks.remove_new_policy_trn_from_s3_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_agg_table_task
AS
REMOVE @control_db.external_stages.policy_trn_stage/new_policy_trn;






--log
CREATE OR REPLACE TASK control_db.tasks.log_new_policy_trn_load_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.remove_new_policy_trn_from_s3_task
AS
insert into mytest_db.policy_trn.orchestration_log
select 
CURRENT_TIMESTAMP() loaddate,
max(transactiondate) transactiondate,
CURRENT_TIMESTAMP() endloaddate
from mytest_db.policy_trn_staging.stg_pt_new_data;

--notification email
CREATE OR REPLACE TASK control_db.tasks.load_complete_notification_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.log_new_policy_trn_load_task
AS
call control_db.sps.load_complete_notification();


--load testing PK
CREATE OR REPLACE TASK control_db.tasks.load_testing_PK_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_complete_notification_task
AS
call control_db.sps.load_testing_PK();

--load testing FK
CREATE OR REPLACE TASK control_db.tasks.load_testing_FK_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_complete_notification_task
AS
call control_db.sps.load_testing_FK();

--load testing
CREATE OR REPLACE TASK control_db.tasks.load_testing_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_complete_notification_task
AS
call control_db.sps.load_testing();


ALTER PIPE control_db.pipes.policy_trn_new_data_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

alter task control_db.tasks.load_testing_PK_task resume;
alter task control_db.tasks.load_testing_FK_task resume;
alter task control_db.tasks.load_testing_task resume;
alter task control_db.tasks.load_complete_notification_task resume;
alter task control_db.tasks.log_new_policy_trn_load_task resume;
alter task control_db.tasks.remove_new_policy_trn_from_s3_task resume;
alter task control_db.tasks.load_agg_table_task resume;
alter task control_db.tasks.load_fact_policytransaction_task resume;
alter task control_db.tasks.load_dim_policy_task resume;
alter task control_db.tasks.load_dim_coverage_task resume;
alter task control_db.tasks.load_dim_limit_task resume;
alter task control_db.tasks.load_dim_deductible_task resume;
alter task control_db.tasks.load_dim_driver_task resume;
alter task control_db.tasks.load_dim_vehicle_task resume;
alter task control_db.tasks.load_new_policy_trn_task resume;






alter task mytest_db.policy_trn_source.extract_policy_trn_from_source_task resume;



alter task mytest_db.policy_trn_source.extract_policy_trn_from_source_task suspend;


ALTER PIPE control_db.pipes.policy_trn_new_data_pipe SET PIPE_EXECUTION_PAUSED = TRUE;


alter task control_db.tasks.load_new_policy_trn_task suspend;
alter task control_db.tasks.load_testing_PK_task suspend;
alter task control_db.tasks.load_testing_FK_task suspend;
alter task control_db.tasks.load_testing_task suspend;
alter task control_db.tasks.load_complete_notification_task suspend;
alter task control_db.tasks.log_new_policy_trn_load_task suspend;
alter task control_db.tasks.remove_new_policy_trn_from_s3_task suspend;
alter task control_db.tasks.load_agg_table_task suspend;
alter task control_db.tasks.load_fact_policytransaction_task suspend;
alter task control_db.tasks.load_dim_policy_task suspend;
alter task control_db.tasks.load_dim_coverage_task suspend;
alter task control_db.tasks.load_dim_limit_task suspend;
alter task control_db.tasks.load_dim_deductible_task suspend;
alter task control_db.tasks.load_dim_driver_task suspend;
alter task control_db.tasks.load_dim_vehicle_task suspend;



USE SCHEMA mytest_db.policy_trn_source;
show tasks;

USE SCHEMA control_db.pipes;
show pipes;

USE SCHEMA control_db.pipes;
select SYSTEM$PIPE_STATUS('policy_trn_new_data_pipe');

USE SCHEMA control_db.tasks;
show tasks;


select * from mytest_db.policy_trn.orchestration_log;

select count(*) from mytest_db.policy_trn.dim_policy;

select * from mytest_db.policy_trn.dim_deductible;