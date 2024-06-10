CREATE OR REPLACE PROCEDURE control_db.sps.load_fact_policytransaction()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
  num_deleted integer default 0;
  num_inserted integer default 0;
BEGIN
/*----------------------DELETE EXISTEN IF ANY FOR THE SAME  TRANSACTION DATES--------------------------------------*/
  DELETE FROM mytest_db.policy_trn.fact_policytransaction f
  WHERE EXISTS( SELECT 1 FROM mytest_db.policy_trn_staging.stg_pt stg Where f.transactiondate = stg.transactiondate);
  
  num_deleted := SQLROWCOUNT;
  
/*---------------------------------------       INSERT NEW       -------------------------------------------------*/
INSERT INTO mytest_db.policy_trn.fact_policytransaction
(
/*-----------------------------------------------------------------------*/
policytransaction_id,
/*-----------------------------------------------------------------------*/        
transactiondate,
accountingdate,
/*-----------------------------------------------------------------------*/
transaction_id,
transactioneffectivedate,
transactionsequence,
/*-----------------------------------------------------------------------*/
policy_id,
veh_effectivedate,
vehicle_id,
driver_id,
coverage_id,
limit_id,
deductible_id,
/*-----------------------------------------------------------------------*/
amount,
loaddate
)
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
coalesce(dim_limit.limit_id, 0) as limit_id,
coalesce(dim_deductible.deductible_id, 0) as deductible_id,
/*-----------------------------------------------------------------------*/
stg.amount
/*-----------------------------------------------------------------------*/
/*=======================================================================*/
from mytest_db.policy_trn_staging.stg_pt_new_data stg
--
left outer join mytest_db.policy_trn.dim_transaction dim_transaction
on stg.transactioncd=dim_transaction.transactioncd
--
left outer join mytest_db.policy_trn.dim_policy dim_policy
on stg.policy_uniqueid=dim_policy.policy_uniqueid
--
left outer join mytest_db.policy_trn.dim_coverage dim_coverage
on stg.coveragecd=dim_coverage.coveragecd
and stg.subline=dim_coverage.subline
and stg.asl=dim_coverage.asl
--
left outer join mytest_db.policy_trn.dim_limit dim_limit
on stg.limit1=dim_limit.limit1
and stg.limit2=dim_limit.limit2
--
left outer join mytest_db.policy_trn.dim_deductible dim_deductible
on stg.deductible1=dim_deductible.deductible1
and stg.deductible2=dim_deductible.deductible2
--
left outer join mytest_db.policy_trn.dim_driver dim_driver
on concat(cast(stg.policy_uniqueid as varchar) , '_' , cast(stg.riskcd2 as varchar) ) = dim_driver.driver_uniqueid
and (stg.drv_effectivedate >= dim_driver.valid_fromdate 
and stg.drv_effectivedate < dim_driver.valid_todate)

--
left outer join mytest_db.policy_trn.dim_vehicle dim_vehicle
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

num_inserted := SQLROWCOUNT;

RETURN 'Deleted: ' ||  num_deleted || '. Inserted: ' || num_inserted || '.';

END;



CREATE OR REPLACE PROCEDURE control_db.sps.load_agg_table(month_id integer)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
  num_deleted integer default 0;
  num_inserted integer default 0;
  rs RESULTSET;
  var_month_id integer;
BEGIN

    
    
  
  rs := (
        select distinct cast(substring(cast( accountingdate as varchar), 1,6) as int) month_id
        from mytest_db.policy_trn.fact_policytransaction 
        where ( cast(substring(cast( accountingdate as varchar), 1,6) as int)=:month_id or :month_id is null )
        );
  
  FOR record IN rs DO
  
    var_month_id := record.month_id;
     
    delete from mytest_db.policy_trn.dummy_agg_table 
    where month_id=:var_month_id;

    num_deleted := num_deleted + SQLROWCOUNT;

    --Insert new

    insert into mytest_db.policy_trn.dummy_agg_table 
    (
     agg_id, 
     month_id,
     policy_id,
     metric,
     loaddate
    )
    select
    cast(substring(cast( accountingdate as varchar), 1,6) as int) agg_id,
    cast(substring(cast( accountingdate as varchar), 1,6) as int) month_id,
    policy_id,
    sum(amount) as metric,
    CURRENT_TIMESTAMP() loaddate
    from mytest_db.policy_trn.fact_policytransaction
    where cast(substring(cast( accountingdate as varchar), 1,6) as int)= :var_month_id
    group by
    cast(substring(cast( accountingdate as varchar), 1,6) as int),
    policy_id;

    num_inserted := num_inserted + SQLROWCOUNT;
    
  END FOR;
    
  RETURN 'Deleted: ' ||  num_deleted || '. Inserted: ' || num_inserted || '.';
  

END;