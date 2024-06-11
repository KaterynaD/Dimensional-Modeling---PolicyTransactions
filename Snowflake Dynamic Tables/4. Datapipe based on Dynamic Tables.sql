-- Create an append-only stream on the staging table.
create or replace  stream mytest_db.policy_trn_staging.stg_pt_append_only_stream on table mytest_db.policy_trn_staging.stg_pt append_only=true;

-- Load into staging table
create or replace pipe control_db.pipes.policy_trn_new_data_pipe auto_ingest=true as
copy into mytest_db.policy_trn_staging.stg_pt 
from @control_db.external_stages.policy_trn_stage/new_policy_trn
file_format = (FORMAT_NAME='control_db.file_formats.csv_format');

ALTER PIPE control_db.pipes.policy_trn_new_data_pipe SET PIPE_EXECUTION_PAUSED = FALSE;



-- Refresh Dynamic tables
CREATE OR REPLACE TASK control_db.tasks.refresh_dynamic_tables_task
    WAREHOUSE = compute_wh 
    WHEN SYSTEM$STREAM_HAS_DATA('mytest_db.policy_trn_staging.stg_pt_append_only_stream') 
AS
BEGIN
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_POLICY REFRESH;
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_COVERAGE REFRESH;
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DEDUCTIBLE REFRESH;
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_LIMIT REFRESH;
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_DRIVER REFRESH;
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.DIM_VEHICLE REFRESH;
 ALTER DYNAMIC TABLE MYTEST_DB.POLICY_TRN_DYNTBL.FACT_POLICYTRANSACTION REFRESH;
END;

--Purge stream for the next load
CREATE OR REPLACE TASK control_db.tasks.purge_dynamic_tables_stream_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.refresh_dynamic_tables_task
AS
CREATE OR REPLACE TEMP TABLE RESET_TBL AS
SELECT * FROM mytest_db.policy_trn_staging.stg_pt_append_only_stream;

--Purge data from S3
CREATE OR REPLACE TASK control_db.tasks.remove_dynamic_tables_from_s3_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.refresh_dynamic_tables_task
AS
REMOVE @control_db.external_stages.policy_trn_stage/new_policy_trn;

--log
CREATE OR REPLACE TASK control_db.tasks.log_dynamic_tables_load_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.remove_dynamic_tables_from_s3_task, control_db.tasks.purge_dynamic_tables_stream_task
AS
insert into mytest_db.policy_trn.orchestration_log
select 
CURRENT_TIMESTAMP() loaddate,
max(transactiondate) transactiondate,
CURRENT_TIMESTAMP() endloaddate
from mytest_db.policy_trn_staging.stg_pt;

--notification email
CREATE OR REPLACE TASK control_db.tasks.load_dynamic_tables_complete_notification_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.log_dynamic_tables_load_task
AS
call control_db.sps.load_complete_dyntbl_notification();


--load testing PK
CREATE OR REPLACE TASK control_db.tasks.load_testing_dynamic_tables_PK_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_dynamic_tables_complete_notification_task
AS
call control_db.sps.load_testing_dyntbl_PK();

--load testing FK
CREATE OR REPLACE TASK control_db.tasks.load_testing_dynamic_tables_FK_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_dynamic_tables_complete_notification_task
AS
call control_db.sps.load_testing_dyntbl_FK();

--load testing
CREATE OR REPLACE TASK control_db.tasks.load_testing_dynamic_tables_task
    WAREHOUSE = compute_wh 
    AFTER control_db.tasks.load_dynamic_tables_complete_notification_task
AS
call control_db.sps.load_testing_dyntbl();


delete from mytest_db.policy_trn.orchestration_log where transactiondate>19000102;
truncate table mytest_db.policy_trn_staging.stg_pt;



alter task mytest_db.policy_trn_source.extract_policy_trn_from_source_task resume;

ALTER PIPE control_db.pipes.policy_trn_new_data_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

alter task mytest_db.policy_trn_source.extract_policy_trn_from_source_task suspend;

ALTER PIPE control_db.pipes.policy_trn_new_data_pipe SET PIPE_EXECUTION_PAUSED = TRUE;



alter task control_db.tasks.load_testing_dynamic_tables_PK_task resume;
alter task control_db.tasks.load_testing_dynamic_tables_FK_task resume;
alter task control_db.tasks.load_testing_dynamic_tables_task resume;
alter task control_db.tasks.remove_dynamic_tables_from_s3_task resume;
alter task control_db.tasks.purge_dynamic_tables_stream_task resume;
alter task control_db.tasks.log_dynamic_tables_load_task resume;
alter task control_db.tasks.load_dynamic_tables_complete_notification_task resume;
alter task control_db.tasks.refresh_dynamic_tables_task resume;




alter task control_db.tasks.refresh_dynamic_tables_task suspend;
alter task control_db.tasks.remove_dynamic_tables_from_s3_task suspend;
alter task control_db.tasks.purge_dynamic_tables_stream_task suspend;
alter task control_db.tasks.log_dynamic_tables_load_task suspend;
alter task control_db.tasks.load_dynamic_tables_complete_notification_task suspend;
alter task control_db.tasks.load_testing_dynamic_tables_PK_task suspend;
alter task control_db.tasks.load_testing_dynamic_tables_FK_task suspend;
alter task control_db.tasks.load_testing_dynamic_tables_task suspend;

USE SCHEMA control_db.tasks;
show tasks;
select * from table(result_scan(last_query_id()))
where "name" like '%DYNAMIC_TABLES%';

USE SCHEMA mytest_db.policy_trn_source;
show tasks;

USE SCHEMA control_db.pipes;
select SYSTEM$PIPE_STATUS('policy_trn_new_data_pipe');

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



select * from mytest_db.policy_trn.orchestration_log;

