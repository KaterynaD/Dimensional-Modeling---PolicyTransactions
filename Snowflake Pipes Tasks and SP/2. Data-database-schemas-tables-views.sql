
CREATE TRANSIENT DATABASE mytest_db;

/*--------------------------------------------------------------------------------------------------------*/
/*----------------------------------- DIM and FACTS in POLICY_TRN SCHEMA ---------------------------------*/
/*--------------------------------------------------------------------------------------------------------*/

CREATE SCHEMA POLICY_TRN;

create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_COVERAGE (
	COVERAGE_ID VARCHAR(100) NOT NULL COMMENT 'The primary key for this table',
	COVERAGECD VARCHAR(20) NOT NULL ,
	SUBLINE VARCHAR(5) NOT NULL ,
	ASL VARCHAR(5) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL,
    CONSTRAINT pk_coverage  PRIMARY KEY ( COVERAGE_ID ) 
)COMMENT='Coverage dimension'
;

INSERT INTO MYTEST_DB.POLICY_TRN.DIM_COVERAGE
select
'Unknown' coverage_id,
'~' coveragecd,
'~' subline,
'~' asl,
CURRENT_TIMESTAMP() loaddate;



create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_DEDUCTIBLE (
	DEDUCTIBLE_ID INTEGER NOT NULL  COMMENT 'The primary key for this table',
	DEDUCTIBLE1 NUMBER(38,0) NOT NULL ,
	DEDUCTIBLE2 NUMBER(38,0) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL,
    CONSTRAINT pk_deductible  PRIMARY KEY ( DEDUCTIBLE_ID )  
)COMMENT='Deductibles dimension'
;

INSERT INTO MYTEST_DB.POLICY_TRN.DIM_DEDUCTIBLE
select
0 deductible_id,
0.0 deductible1,
0.0 deductible2,
CURRENT_TIMESTAMP() loaddate;

create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_DRIVER (
	DRIVER_ID VARCHAR(100) NOT NULL  COMMENT 'The primary key for this table',
	VALID_FROMDATE TIMESTAMP_NTZ(9) NOT NULL ,
	VALID_TODATE TIMESTAMP_NTZ(9) NOT NULL ,
	DRIVER_UNIQUEID VARCHAR(100) NOT NULL ,
	GENDERCD VARCHAR(10) NOT NULL ,
	BIRTHDATE VARCHAR(10) NOT NULL ,
	MARITALSTATUSCD VARCHAR(10) NOT NULL ,
	POINTSCHARGED VARCHAR(3) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL ,
    CONSTRAINT pk_driver  PRIMARY KEY ( DRIVER_ID )  
)COMMENT='Driver scd2 dimension'
;

INSERT INTO MYTEST_DB.POLICY_TRN.DIM_DRIVER
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

create or replace sequence DIM_LIMIT_SEQ start with 1 increment by 1 noorder;

create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_LIMIT (
	LIMIT_ID INTEGER NOT NULL  COMMENT 'The primary key for this table',
	LIMIT1 VARCHAR(100) NOT NULL ,
	LIMIT2 VARCHAR(100) NOT NULL  ,
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL ,
    CONSTRAINT pk_limit  PRIMARY KEY ( LIMIT_ID )  
)COMMENT='Limits dimension'
;

INSERT INTO MYTEST_DB.POLICY_TRN.DIM_LIMIT
select
0 limit_id,
'~' limit1,
'~' limit2,
CURRENT_TIMESTAMP() loaddate;


create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_POLICY (
	POLICY_ID INTEGER NOT NULL  COMMENT 'The primary key for this table',
	POLICY_UNIQUEID NUMBER(38,0) NOT NULL ,
	POLICYNUMBER VARCHAR(20) NOT NULL ,
	EFFECTIVEDATE DATE NOT NULL ,
	EXPIRATIONDATE DATE NOT NULL ,
	INCEPTIONDATE DATE NOT NULL ,
	POLICYSTATE VARCHAR(2) NOT NULL ,
	CARRIERCD VARCHAR(10) NOT NULL ,
	COMPANYCD VARCHAR(10) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(9),
    CONSTRAINT pk_policy  PRIMARY KEY ( POLICY_ID )  
)COMMENT='Auto policy dimension'
;

INSERT INTO MYTEST_DB.POLICY_TRN.DIM_POLICY
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


create or replace TABLE MYTEST_DB.POLICY_TRN.DIM_VEHICLE (
	VEHICLE_ID VARCHAR(100) NOT NULL  COMMENT 'The primary key for this table',
	VALID_FROMDATE TIMESTAMP_NTZ(9) NOT NULL ,
	VALID_TODATE TIMESTAMP_NTZ(9) NOT NULL ,
	VEHICLE_UNIQUEID VARCHAR(100) NOT NULL ,
	VIN VARCHAR(20) NOT NULL ,
	MODEL VARCHAR(100) NOT NULL ,
	MODELYR VARCHAR(100) NOT NULL ,
	MANUFACTURER VARCHAR(100) NOT NULL ,
	ESTIMATEDANNUALDISTANCE VARCHAR(100) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL ,
    CONSTRAINT pk_vehicle  PRIMARY KEY ( VEHICLE_ID )
)COMMENT='Vehicle scd2 dimension'
;

INSERT INTO MYTEST_DB.POLICY_TRN.DIM_VEHICLE
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



create or replace TABLE MYTEST_DB.POLICY_TRN.DUMMY_AGG_TABLE (
	AGG_ID INTEGER NOT NULL COMMENT 'It''s based on accountingdate and can be duplicated',
	MONTH_ID INTEGER NOT NULL ,
	POLICY_ID INTEGER NOT NULL ,
	METRIC NUMBER(38,13) NOT NULL ,
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL ,
    CONSTRAINT FK_AGG_FOR_POLICY FOREIGN KEY ( POLICY_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_POLICY ( POLICY_ID ) RELY   COMMENT  'Agg based policy. A policy can have one or many aggregations.'
)COMMENT='Demo table with a complex aggregation based on Fact_Policytransaction'
;

create or replace TABLE MYTEST_DB.POLICY_TRN.FACT_POLICYTRANSACTION (
	POLICYTRANSACTION_ID VARCHAR(100) NOT NULL  COMMENT 'It''s based on transactiondate and can be duplicated',
	TRANSACTIONDATE NUMBER(38,0) NOT NULL  COMMENT 'Transaction date',
	ACCOUNTINGDATE NUMBER(38,0) NOT NULL  COMMENT 'Accounting date',
	TRANSACTION_ID INTEGER NOT NULL ,
	TRANSACTIONEFFECTIVEDATE NUMBER(38,0)  NOT NULL COMMENT 'Transaction Effective date',
	TRANSACTIONSEQUENCE NUMBER(38,0)  NOT NULL  COMMENT 'Transaction Sequence Number per Policy term',
	POLICY_ID INTEGER NOT NULL ,
	VEH_EFFECTIVEDATE TIMESTAMP_NTZ(9) NOT NULL ,
	VEHICLE_ID VARCHAR(100) NOT NULL ,
	DRIVER_ID VARCHAR(100) NOT NULL ,
	COVERAGE_ID VARCHAR(100) NOT NULL ,
	LIMIT_ID INTEGER NOT NULL ,
	DEDUCTIBLE_ID INTEGER NOT NULL ,
	AMOUNT NUMBER(38,13) NOT NULL COMMENT 'Transaction Amount',
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL,
    CONSTRAINT FK_TRANSACTION_IS_TYPE FOREIGN KEY ( TRANSACTION_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_TRANSACTION ( TRANSACTION_ID ) RELY   COMMENT  'Transaction`s type. A type can have zero or many transactions.',
    CONSTRAINT FK_TRANSACTION_FOR_POLICY FOREIGN KEY ( POLICY_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_POLICY ( POLICY_ID ) RELY   COMMENT  'Transaction`s based policy. A policy can have one or many transactions.',
    CONSTRAINT FK_TRANSACTION_FOR_VEHICLE FOREIGN KEY ( VEHICLE_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_VEHICLE ( VEHICLE_ID ) RELY   COMMENT  'Transaction`s based vehicle. A vehicle can have one or many transactions.',
    CONSTRAINT FK_TRANSACTION_FOR_DRIVER FOREIGN KEY ( DRIVER_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_DRIVER ( DRIVER_ID ) RELY   COMMENT  'Transaction`s based driver. A driver can have zero or many transactions. Drivers without assigned vehicles may not have transactions',    
    CONSTRAINT FK_TRANSACTION_SETs_COVERAGE FOREIGN KEY ( COVERAGE_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_COVERAGE ( COVERAGE_ID ) RELY   COMMENT  'A coverage is set by a transaction. A coverage always has at least one transaction',    
    CONSTRAINT FK_TRANSACTION_SETs_LIMIT FOREIGN KEY ( LIMIT_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_LIMIT ( LIMIT_ID ) RELY   COMMENT  'A limit is set by a transaction. A limit always has at least one transaction',  
    CONSTRAINT FK_TRANSACTION_SETs_DEDUCTIBLE FOREIGN KEY ( DEDUCTIBLE_ID ) REFERENCES MYTEST_DB.POLICY_TRN.DIM_DEDUCTIBLE ( DEDUCTIBLE_ID ) RELY   COMMENT  'A deductible is set by a transaction. A deductible always has at least one transaction'    
)COMMENT='Policy Transaction Facy Table'
;



create or replace TABLE MYTEST_DB.POLICY_TRN.ORCHESTRATION_LOG (
	LOADDATE TIMESTAMP_NTZ(9) NOT NULL  COMMENT 'Timestamp of the load start. It''s a primary key.',
	TRANSACTIONDATE INTEGER COMMENT 'The latest Transaction Date in this load.',
	ENDLOADDATE TIMESTAMP_NTZ(9) COMMENT 'Timestamp of the load end'
)COMMENT='Orchestration load details'
;

INSERT INTO MYTEST_DB.POLICY_TRN.ORCHESTRATION_LOG
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

CREATE SCHEMA POLICY_TRN_STAGING;  



create or replace TABLE MYTEST_DB.POLICY_TRN_STAGING.STG_PT (
	POLICYSTATE VARCHAR(2),
	CARRIERCD VARCHAR(10),
	COMPANYCD VARCHAR(5),
	POLICYNUMBER VARCHAR(10),
	POLICY_UNIQUEID NUMBER(38,0),
	EFFECTIVEDATE DATE,
	EXPIRATIONDATE DATE,
	INCEPTIONDATE DATE,
	TRANSACTIONDATE NUMBER(38,0),
	ACCOUNTINGDATE NUMBER(38,0),
	TRANSACTIONEFFECTIVEDATE NUMBER(38,0),
	TRANSACTIONCD VARCHAR(20),
	TRANSACTIONSEQUENCE NUMBER(38,0),
	RISKCD1 VARCHAR(5),
	RISKCD2 VARCHAR(5),
	COVERAGECD VARCHAR(20),
	SUBLINE VARCHAR(5),
	ASL VARCHAR(5),
	LIMIT1 VARCHAR(20),
	LIMIT2 VARCHAR(20),
	DEDUCTIBLE1 NUMBER(38,0),
	DEDUCTIBLE2 NUMBER(38,0),
	AMOUNT NUMBER(38,0),
	MODEL VARCHAR(100),
	MODELYR VARCHAR(4),
	MANUFACTURER VARCHAR(100),
	ESTIMATEDANNUALDISTANCE NUMBER(38,0),
	VIN VARCHAR(20),
	GENDERCD VARCHAR(10),
	BIRTHDATE DATE,
	MARITALSTATUSCD VARCHAR(10),
	POINTSCHARGEDTERM NUMBER(38,0),
	VEH_EFFECTIVEDATE TIMESTAMP_NTZ(9),
	DRV_EFFECTIVEDATE TIMESTAMP_NTZ(9)
);

create or replace TRANSIENT TABLE MYTEST_DB.POLICY_TRN_STAGING.STG_PT_NEW_DATA (
	POLICYSTATE VARCHAR(2),
	CARRIERCD VARCHAR(10),
	COMPANYCD VARCHAR(5),
	POLICYNUMBER VARCHAR(10),
	POLICY_UNIQUEID NUMBER(38,0),
	EFFECTIVEDATE DATE,
	EXPIRATIONDATE DATE,
	INCEPTIONDATE DATE,
	TRANSACTIONDATE NUMBER(38,0),
	ACCOUNTINGDATE NUMBER(38,0),
	TRANSACTIONEFFECTIVEDATE NUMBER(38,0),
	TRANSACTIONCD VARCHAR(20),
	TRANSACTIONSEQUENCE NUMBER(38,0),
	RISKCD1 VARCHAR(5),
	RISKCD2 VARCHAR(5),
	COVERAGECD VARCHAR(20),
	SUBLINE VARCHAR(5),
	ASL VARCHAR(5),
	LIMIT1 VARCHAR(20),
	LIMIT2 VARCHAR(20),
	DEDUCTIBLE1 NUMBER(38,0),
	DEDUCTIBLE2 NUMBER(38,0),
	AMOUNT NUMBER(38,0),
	MODEL VARCHAR(100),
	MODELYR VARCHAR(4),
	MANUFACTURER VARCHAR(100),
	ESTIMATEDANNUALDISTANCE NUMBER(38,0),
	VIN VARCHAR(20),
	GENDERCD VARCHAR(10),
	BIRTHDATE DATE,
	MARITALSTATUSCD VARCHAR(10),
	POINTSCHARGEDTERM NUMBER(38,0),
	VEH_EFFECTIVEDATE TIMESTAMP_NTZ(9),
	DRV_EFFECTIVEDATE TIMESTAMP_NTZ(9)
);


create or replace view MYTEST_DB.POLICY_TRN_STAGING.STG_DRIVER(
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
from mytest_db.policy_trn_staging.stg_pt_new_data stg
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

create or replace view MYTEST_DB.POLICY_TRN_STAGING.STG_VEHICLE(
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
from mytest_db.policy_trn_staging.stg_pt_new_data stg


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

/*--------------------------------------------------------------------------------------------------------*/
/*----------------------------------- Simulating source data from a transactional system -----------------*/
/*--------------------------------------------------------------------------------------------------------*/

CREATE SCHEMA POLICY_TRN_SOURCE;  


--one time copy from file
create or replace TABLE MYTEST_DB.POLICY_TRN_SOURCE.STG_PT (
	POLICYSTATE VARCHAR(2),
	CARRIERCD VARCHAR(10),
	COMPANYCD VARCHAR(5),
	POLICYNUMBER VARCHAR(10),
	POLICY_UNIQUEID NUMBER(38,0),
	EFFECTIVEDATE DATE,
	EXPIRATIONDATE DATE,
	INCEPTIONDATE DATE,
	TRANSACTIONDATE NUMBER(38,0),
	ACCOUNTINGDATE NUMBER(38,0),
	TRANSACTIONEFFECTIVEDATE NUMBER(38,0),
	TRANSACTIONCD VARCHAR(20),
	TRANSACTIONSEQUENCE NUMBER(38,0),
	RISKCD1 VARCHAR(5),
	RISKCD2 VARCHAR(5),
	COVERAGECD VARCHAR(20),
	SUBLINE VARCHAR(5),
	ASL VARCHAR(5),
	LIMIT1 VARCHAR(20),
	LIMIT2 VARCHAR(20),
	DEDUCTIBLE1 NUMBER(38,0),
	DEDUCTIBLE2 NUMBER(38,0),
	AMOUNT NUMBER(38,0),
	MODEL VARCHAR(100),
	MODELYR VARCHAR(4),
	MANUFACTURER VARCHAR(100),
	ESTIMATEDANNUALDISTANCE NUMBER(38,0),
	VIN VARCHAR(20),
	GENDERCD VARCHAR(10),
	BIRTHDATE DATE,
	MARITALSTATUSCD VARCHAR(10),
	POINTSCHARGEDTERM NUMBER(38,0),
	VEH_EFFECTIVEDATE TIMESTAMP_NTZ(9),
	DRV_EFFECTIVEDATE TIMESTAMP_NTZ(9)
);
