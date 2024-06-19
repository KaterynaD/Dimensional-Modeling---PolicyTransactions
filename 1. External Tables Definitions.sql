--not related to a specific DB/schema
CREATE OR REPLACE EXTERNAL VOLUME iceberg_vol
   STORAGE_LOCATIONS =
   (
    (
     NAME = 'iceberg_test'
     STORAGE_PROVIDER = 'S3'
     STORAGE_BASE_URL = 's3://kd-projects/other/iceberg/'
     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::757107622481:role/snowflake-iceberg-role'
    )
   );

--describe external volume iceberg_vol;
/*--------------------------------------------------------------------------------------------------------*/
/*----------------------------------- External DIM and FACTS in POLICY_TRN SCHEMA ------------------------*/
/*--------------------------------------------------------------------------------------------------------*/

CREATE SCHEMA MYTEST_DB.POLICY_TRN_EXTTBL;

create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DIM_COVERAGE (
	COVERAGE_ID VARCHAR NOT NULL COMMENT 'The primary key for this table',
	COVERAGECD VARCHAR NOT NULL ,
	SUBLINE VARCHAR NOT NULL ,
	ASL VARCHAR NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL,
    CONSTRAINT pk_coverage  PRIMARY KEY ( COVERAGE_ID ) RELY
)COMMENT='Coverage dimension'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DIM_COVERAGE'
;

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.DIM_COVERAGE
select
'Unknown' coverage_id,
'~' coveragecd,
'~' subline,
'~' asl,
CURRENT_TIMESTAMP() loaddate;



create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DIM_DEDUCTIBLE (
	DEDUCTIBLE_ID INTEGER NOT NULL  COMMENT 'The primary key for this table',
	DEDUCTIBLE1 NUMBER(38,0) NOT NULL ,
	DEDUCTIBLE2 NUMBER(38,0) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL,
    CONSTRAINT pk_deductible  PRIMARY KEY ( DEDUCTIBLE_ID )   RELY
)COMMENT='Deductibles dimension'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DIM_DEDUCTIBLE';

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.DIM_DEDUCTIBLE
select
0 deductible_id,
0.0 deductible1,
0.0 deductible2,
CURRENT_TIMESTAMP() loaddate;

create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DIM_DRIVER (
	DRIVER_ID VARCHAR NOT NULL  COMMENT 'The primary key for this table',
	VALID_FROMDATE TIMESTAMP_NTZ(6) NOT NULL ,
	VALID_TODATE TIMESTAMP_NTZ(6) NOT NULL ,
	DRIVER_UNIQUEID VARCHAR NOT NULL ,
	GENDERCD VARCHAR NOT NULL ,
	BIRTHDATE VARCHAR NOT NULL ,
	MARITALSTATUSCD VARCHAR NOT NULL ,
	POINTSCHARGED VARCHAR NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL ,
    CONSTRAINT pk_driver  PRIMARY KEY ( DRIVER_ID )   RELY
)COMMENT='Driver scd2 dimension'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DIM_DRIVER';

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.DIM_DRIVER
select
'Unknown' driver_id,
'1900-01-01' valid_fromdate,
'3000-01-01' valid_todate,
'Unknown' driver_uniqueid,
'~' gendercd,
'1900-01-01' birthdate,
'~' maritalstatuscd,
0 pointscharged,
CURRENT_TIMESTAMP() loaddate;

create or replace sequence MYTEST_DB.PUBLIC.DIM_LIMIT_SEQ start with 1 increment by 1 noorder;

create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DIM_LIMIT (
	LIMIT_ID INTEGER NOT NULL  COMMENT 'The primary key for this table',
	LIMIT1 VARCHAR NOT NULL ,
	LIMIT2 VARCHAR NOT NULL  ,
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL ,
    CONSTRAINT pk_limit  PRIMARY KEY ( LIMIT_ID )   RELY
)COMMENT='Limits dimension'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DIM_LIMIT';

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.DIM_LIMIT
select
0 limit_id,
'~' limit1,
'~' limit2,
CURRENT_TIMESTAMP() loaddate;


create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DIM_POLICY (
	POLICY_ID INTEGER NOT NULL  COMMENT 'The primary key for this table',
	POLICY_UNIQUEID INTEGER NOT NULL ,
	POLICYNUMBER VARCHAR NOT NULL ,
	EFFECTIVEDATE DATE NOT NULL ,
	EXPIRATIONDATE DATE NOT NULL ,
	INCEPTIONDATE DATE NOT NULL ,
	POLICYSTATE VARCHAR NOT NULL ,
	CARRIERCD VARCHAR NOT NULL ,
	COMPANYCD VARCHAR NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(6),
    CONSTRAINT pk_policy  PRIMARY KEY ( POLICY_ID )   RELY
)COMMENT='Auto policy dimension'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DIM_POLICY';

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.DIM_POLICY
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
CURRENT_TIMESTAMP() loaddate;


create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DIM_VEHICLE (
	VEHICLE_ID VARCHAR NOT NULL  COMMENT 'The primary key for this table',
	VALID_FROMDATE TIMESTAMP_NTZ(6) NOT NULL ,
	VALID_TODATE TIMESTAMP_NTZ(6) NOT NULL ,
	VEHICLE_UNIQUEID VARCHAR NOT NULL ,
	VIN VARCHAR NOT NULL ,
	MODEL VARCHAR NOT NULL ,
	MODELYR VARCHAR NOT NULL ,
	MANUFACTURER VARCHAR NOT NULL ,
	ESTIMATEDANNUALDISTANCE VARCHAR NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL ,
    CONSTRAINT pk_vehicle  PRIMARY KEY ( VEHICLE_ID )  RELY
)COMMENT='Vehicle scd2 dimension'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DIM_VEHICLE';

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.DIM_VEHICLE
select
'Unknown' vehicle_id,
'1900-01-01' valid_fromdate,
'3000-12-31' valid_todate,
'Unknown' vehicle_uniqueid,
'~' vin,
'~' model,
'~' modelyr,
'~' manufacturer,
'0' estimatedannualdistance,
CURRENT_TIMESTAMP() loaddate;

/*
create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_TRANSACTION (
	TRANSACTION_ID NUMBER(38,0),
	TRANSACTIONCD VARCHAR(50)
);

ALTER table MYTEST_DB.POLICY_TRN.DIM_TRANSACTION add CONSTRAINT pk_transaction  PRIMARY KEY ( TRANSACTION_ID );
--copy from file
transaction_id,transactioncd
0,Unknown
1,Unapply
2,Non-Renewal Rescind
3,Reinstatement
4,Renewal
5,New Business
6,Cancellation
7,Endorsement
8,Non-Renewal
9,Commission Reversal
*/



create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.DUMMY_AGG_TABLE (
	AGG_ID INTEGER NOT NULL COMMENT 'It''s based on accountingdate and can be duplicated',
	MONTH_ID INTEGER NOT NULL ,
	POLICY_ID INTEGER NOT NULL ,
	METRIC NUMBER(38,13) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL ,
    CONSTRAINT FK_AGG_FOR_POLICY FOREIGN KEY ( POLICY_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_POLICY ( POLICY_ID ) RELY   COMMENT  'Agg based policy. A policy can have one or many aggregations.'
)COMMENT='Demo table with a complex aggregation based on Fact_Policytransaction'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/DUMMY_AGG_TABLE';

create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.FACT_POLICYTRANSACTION (
	POLICYTRANSACTION_ID VARCHAR NOT NULL  COMMENT 'It''s based on transactiondate and can be duplicated',
	TRANSACTIONDATE INTEGER NOT NULL  COMMENT 'Transaction date',
	ACCOUNTINGDATE INTEGER NOT NULL  COMMENT 'Accounting date',
	TRANSACTION_ID NUMBER(38,0) NOT NULL ,
	TRANSACTIONEFFECTIVEDATE INTEGER  NOT NULL COMMENT 'Transaction Effective date',
	TRANSACTIONSEQUENCE INTEGER  NOT NULL  COMMENT 'Transaction Sequence Number per Policy term',
	POLICY_ID INTEGER NOT NULL ,
	VEH_EFFECTIVEDATE TIMESTAMP_NTZ(6) NOT NULL ,
	VEHICLE_ID VARCHAR NOT NULL ,
	DRIVER_ID VARCHAR NOT NULL ,
	COVERAGE_ID VARCHAR NOT NULL ,
	LIMIT_ID INTEGER NOT NULL ,
	DEDUCTIBLE_ID INTEGER NOT NULL ,
	AMOUNT NUMBER(38,13) NOT NULL COMMENT 'Transaction Amount',
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL,
    CONSTRAINT FK_TRANSACTION_IS_TYPE FOREIGN KEY ( TRANSACTION_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_TRANSACTION ( TRANSACTION_ID ) RELY   COMMENT  'Transaction`s type. A type can have zero or many transactions.',
    CONSTRAINT FK_TRANSACTION_FOR_POLICY FOREIGN KEY ( POLICY_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_POLICY ( POLICY_ID ) RELY   COMMENT  'Transaction`s based policy. A policy can have one or many transactions.',
    CONSTRAINT FK_TRANSACTION_FOR_VEHICLE FOREIGN KEY ( VEHICLE_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_VEHICLE ( VEHICLE_ID ) RELY   COMMENT  'Transaction`s based vehicle. A vehicle can have one or many transactions.',
    CONSTRAINT FK_TRANSACTION_FOR_DRIVER FOREIGN KEY ( DRIVER_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_DRIVER ( DRIVER_ID ) RELY   COMMENT  'Transaction`s based driver. A driver can have zero or many transactions. Drivers without assigned vehicles may not have transactions',    
    CONSTRAINT FK_TRANSACTION_SETs_COVERAGE FOREIGN KEY ( COVERAGE_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_COVERAGE ( COVERAGE_ID ) RELY   COMMENT  'A coverage is set by a transaction. A coverage always has at least one transaction',    
    CONSTRAINT FK_TRANSACTION_SETs_LIMIT FOREIGN KEY ( LIMIT_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_LIMIT ( LIMIT_ID ) RELY   COMMENT  'A limit is set by a transaction. A limit always has at least one transaction',  
    CONSTRAINT FK_TRANSACTION_SETs_DEDUCTIBLE FOREIGN KEY ( DEDUCTIBLE_ID ) REFERENCES MYTEST_DB.POLICY_TRN_EXTTBL.DIM_DEDUCTIBLE ( DEDUCTIBLE_ID ) RELY   COMMENT  'A deductible is set by a transaction. A deductible always has at least one transaction'    
)COMMENT='Policy Transaction Facy Table'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/FACT_POLICYTRANSACTION';



create or replace ICEBERG TABLE MYTEST_DB.POLICY_TRN_EXTTBL.ORCHESTRATION_LOG (
	LOADDATE TIMESTAMP_NTZ(6) NOT NULL  COMMENT 'Timestamp of the load start. It''s a primary key.',
	TRANSACTIONDATE INTEGER COMMENT 'The latest Transaction Date in this load.',
	ENDLOADDATE TIMESTAMP_NTZ(6) COMMENT 'Timestamp of the load end'
)COMMENT='Orchestration load details'
CATALOG='SNOWFLAKE'
EXTERNAL_VOLUME='iceberg_vol'
BASE_LOCATION='POLICY_TRN/ORCHESTRATION_LOG';

INSERT INTO MYTEST_DB.POLICY_TRN_EXTTBL.ORCHESTRATION_LOG
select
CURRENT_TIMESTAMP() loaddate,
19000102 transactiondate,
CURRENT_TIMESTAMP() endloaddate;

/* 
create or replace TRANSIENT TABLE MYTEST_DB.POLICY_TRN.ORCHESTRATION_HISTORY (
	TRANSACTIONDATE INTEGER
);

--copy from file
transactiondate
19000102
20200105
20200115
20200131
20200215
*/

/*--------------------------------------------------------------------------------------------------------*/
/*----------------------------------- Staging Data Extracting From Source System  ------------------------*/
/*--------------------------------------------------------------------------------------------------------*/

CREATE SCHEMA MYTEST_DB.POLICY_TRN_STAGING_EXTTBL;  

/*no changes in the staging table. just new files added*/
CREATE OR REPLACE EXTERNAL TABLE MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT (
file_name_part varchar AS SUBSTR(metadata$filename,27,8),
POLICYSTATE VARCHAR(2)	 as 	(value:c1	::string),
CARRIERCD VARCHAR(10)	 as 	(value:c2	::string),
COMPANYCD VARCHAR(5)	 as 	(value:c3	::string),
POLICYNUMBER VARCHAR(10)	 as 	(value:c4	::string),
POLICY_UNIQUEID INTEGER	 as 	(value:c5	::INTEGER),
EFFECTIVEDATE DATE	 as 	(value:c6	::DATE),
EXPIRATIONDATE DATE	 as 	(value:c7	::DATE),
INCEPTIONDATE DATE	 as 	(value:c8	::DATE),
TRANSACTIONDATE INTEGER	 as 	(value:c9	::INTEGER),
ACCOUNTINGDATE  INTEGER	 as 	(value:c10	::INTEGER),
TRANSACTIONEFFECTIVEDATE INTEGER	 as 	(value:c11	::INTEGER),
TRANSACTIONCD VARCHAR(20)	 as 	(value:c12	::string),
TRANSACTIONSEQUENCE INTEGER	 as 	(value:c13	::INTEGER),
RISKCD1 VARCHAR(5)	 as 	(value:c14	::string),
RISKCD2 VARCHAR(5)	 as 	(value:c15	::string),
COVERAGECD VARCHAR(20)	 as 	(value:c16	::string),
SUBLINE VARCHAR(5)	 as 	(value:c17	::string),
ASL VARCHAR(5)	 as 	(value:c18	::string),
LIMIT1 VARCHAR(20)	 as 	(value:c19	::string),
LIMIT2 VARCHAR(20)	 as 	(value:c20	::string),
DEDUCTIBLE1 INTEGER	 as 	(value:c21	::INTEGER),
DEDUCTIBLE2 INTEGER	 as 	(value:c22	::INTEGER),
AMOUNT NUMBER(38,0)	 as 	(value:c23	::NUMBER(38,0)),
MODEL VARCHAR(100)	 as 	(value:c24	::string),
MODELYR VARCHAR(4)	 as 	(value:c25	::string),
MANUFACTURER VARCHAR(100)	 as 	(value:c26	::string),
ESTIMATEDANNUALDISTANCE INTEGER	 as 	(value:c27	::INTEGER),
VIN VARCHAR(20)	 as 	(value:c28	::string),
GENDERCD VARCHAR(10)	 as 	(value:c29	::string),
BIRTHDATE DATE	 as 	(value:c30	::DATE),
MARITALSTATUSCD VARCHAR(10)	 as 	(value:c31	::string),
POINTSCHARGEDTERM INTEGER	 as 	(value:c32	::INTEGER),
VEH_EFFECTIVEDATE TIMESTAMP_NTZ(9)	 as 	(value:c33	::TIMESTAMP_NTZ(9)),
DRV_EFFECTIVEDATE TIMESTAMP_NTZ(9)	 as 	(value:c34	::TIMESTAMP_NTZ(9))
)
PARTITION BY (file_name_part)
WITH LOCATION = @control_db.external_stages.policy_trn_stage/staging/stg_pt/
FILE_FORMAT = (FORMAT_NAME='control_db.file_formats.csv_format' error_on_column_count_mismatch=false );  



--Can not use Automatic Refresh because there is always a short delay and next task may start before the refresh

--ALTER EXTERNAL TABLE MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT REFRESH;

create or replace view MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA
as
select * from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT
where cast(file_name_part as int) = 
(select max(transactiondate) from  MYTEST_DB.POLICY_TRN_EXTTBL.ORCHESTRATION_LOG where endloaddate is null);


create or replace view MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_DRIVER(
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
from  MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg
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

create or replace view MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_VEHICLE(
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
from MYTEST_DB.POLICY_TRN_STAGING_EXTTBL.STG_PT_NEW_DATA stg


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