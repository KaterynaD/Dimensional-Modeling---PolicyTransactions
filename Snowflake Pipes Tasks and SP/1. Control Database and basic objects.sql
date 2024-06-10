---> set the Warehouse
USE WAREHOUSE compute_wh;

create or replace transient database control_db;

USE database control_db;

create or replace schema external_stages;

create or replace schema internal_stages;

create or replace schema file_formats;

create or replace schema integrations;

create or replace schema sps;

create or replace schema tasks;

create or replace schema streams;

create or replace schema pipes;

USE schema integrations;
--no db or schema names
CREATE OR REPLACE STORAGE INTEGRATION aws_kd_projects_stg_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::757107622481:role/Snowflake_Access'
  STORAGE_ALLOWED_LOCATIONS = ('*');


create or replace stage control_db.external_stages.policy_trn_stage 
STORAGE_INTEGRATION = aws_kd_projects_stg_int
url='s3://kd-projects/policy_trn/';

create or replace file format control_db.file_formats.csv_format_not_compressed
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true;

