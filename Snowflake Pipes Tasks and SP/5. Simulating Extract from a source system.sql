
--Extract from a source table
CREATE OR REPLACE TASK mytest_db.policy_trn_source.extract_policy_trn_from_source_task
    WAREHOUSE = compute_wh 
    SCHEDULE = '3 minute'
AS
DECLARE 
  query VARCHAR; 
BEGIN 
QUERY := 
' copy into @control_db.external_stages.policy_trn_stage/new_policy_trn_' || TO_CHAR(current_timestamp(),'yyyymmddhh24miss') ||
'             from ( ' ||
'  with latest_loaded_transactiondate as ' ||
' (SELECT transactiondate  ' ||
'  FROM mytest_db.policy_trn.orchestration_log /*Latest Loaded transactiondate*/ ' ||
'  WHERE LoadDate=(select max(t.loaddate) from mytest_db.policy_trn.orchestration_log t where t.endloaddate is not null)) ' ||
' ,new_transactiondate as ( ' ||
' SELECT min(h.transactiondate) transactiondate  ' ||
' FROM mytest_db.policy_trn.orchestration_history h ' ||
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

EXECUTE IMMEDIATE (:QUERY);


END; 


alter task mytest_db.policy_trn_source.extract_policy_trn_from_source_task resume;

alter task mytest_db.policy_trn_source.extract_policy_trn_from_source_task suspend;




 