CREATE OR REPLACE PROCEDURE control_db.sps.load_testing_PK()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
data varchar default '<H2>Something went wrong...</H2>';
constraint_name varchar ;
test_sql  varchar ;
rs RESULTSET;
rsi RESULTSET;
email varchar default 'drogaieva@gmail.com';
subject varchar default 'Policy Trn Load PK uniqeuness Testing';
Flag boolean default False;
BEGIN

--PK testing assuming there is 1 column per constraint only
show primary keys in database mytest_db;

rs := (
select "constraint_name" as constraint_name, 'select count(*) cnt from (select '|| "column_name" || ' from ' || "database_name" || '.' || "schema_name" || '.' || "table_name" || ' group by ' || "column_name" || ' having count(*)>1) data;' as test_SQL
from table(result_scan(last_query_id()))
        );
  
FOR record IN rs DO
  
    constraint_name := record.constraint_name;
    test_sql := record.test_SQL;
    rsi := (EXECUTE IMMEDIATE test_sql);

    FOR record_i IN rsi DO
    
     IF (record_i.cnt!=0) THEN 
      data := data || '<li>' || constraint_name || ' is INVALID </li>';
      Flag := True;
     END IF;
    END FOR;
 
END FOR;



 IF (Flag) THEN
  CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION('{ "text/html": "' || :data || '"}','{"my_email_int": {"subject": "' || :subject ||'","toAddress": ["' || :email ||'"]}}');
 END IF;

  
 return Flag;
END;



CREATE OR REPLACE PROCEDURE control_db.sps.load_testing_FK()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
data varchar default '<H2>Something went wrong...</H2>';
constraint_name varchar ;
test_sql  varchar ;
rs RESULTSET;
rsi RESULTSET;
email varchar default 'drogaieva@gmail.com';
subject varchar default 'Policy Trn Load FK inegrity Testing';
Flag boolean default False;
BEGIN


--FK testing assuming there is 1 column per constraint only

show IMPORTED keys in database mytest_db;

rs := (
select "fk_name" as constraint_name, 'select  count(*) cnt from ( select ' || "fk_column_name" || ' from ' || "fk_database_name" || '.' || "fk_schema_name" || '.' || "fk_table_name" || ' except select ' || "pk_column_name" || ' from ' || "pk_database_name" || '.' || "pk_schema_name" || '.' || "pk_table_name" || ' ) data;' as test_SQL
from table(result_scan(last_query_id()))
        );
  
FOR record IN rs DO
  
    constraint_name := record.constraint_name;
    test_sql := record.test_SQL;
    rsi := (EXECUTE IMMEDIATE test_sql);

    FOR record_i IN rsi DO
    
     IF (record_i.cnt!=0) THEN 
      data := data || '<li>' || constraint_name || ' is INVALID </li>';
      Flag := True;
     END IF;
    END FOR;
 
END FOR;

 IF (Flag) THEN
  CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION('{ "text/html": "' || :data || '"}','{"my_email_int": {"subject": "' || :subject ||'","toAddress": ["' || :email ||'"]}}');
 END IF;

  
 return Flag;
END;



CREATE OR REPLACE PROCEDURE control_db.sps.load_testing()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
data varchar default '<H2>Something went wrong...</H2>';
constraint_name varchar ;
test_sql  varchar ;
rs RESULTSET;
rsi RESULTSET;
email varchar default 'drogaieva@gmail.com';
subject varchar default 'Policy Trn Load Testing';
Flag boolean default False;
BEGIN

 --Other tests

 rs := (with staging as 
(
select count(*) cnt ,
sum(stg.amount) sum_amount
from mytest_db.policy_trn_staging.stg_pt_new_data stg
)
, fact as 
(
select count(*) cnt  ,
sum(amount) sum_amount
from mytest_db.policy_trn.fact_policytransaction 
where 
    loaddate = (select max(loaddate) from mytest_db.policy_trn.fact_policytransaction )
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
staging.sum_amount <> fact.sum_amount);

FOR record IN rs DO
 data := data || '<li> Count records or Amounts in Fact table and staging do not match!</li>';
 Flag := True;
END FOR;


 IF (Flag) THEN
  CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION('{ "text/html": "' || :data || '"}','{"my_email_int": {"subject": "' || :subject ||'","toAddress": ["' || :email ||'"]}}');
 END IF;

  
 return Flag;
END;


