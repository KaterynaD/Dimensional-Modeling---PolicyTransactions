USE schema control_db.integrations;
CREATE OR REPLACE NOTIFICATION INTEGRATION my_email_int
  TYPE=EMAIL
  ENABLED=TRUE;
  


CREATE OR REPLACE PROCEDURE control_db.sps.load_complete_notification()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
DECLARE
query varchar;
table_name varchar;
data varchar default '<H2>Today''s data:</H2><TABLE border=1><tr></tr><th>Table Name</th><th>Count Records on Latest LoadDate</th><th>Latest LoadDate</th><th>Total Records</th></tr>';
rs RESULTSET;
rsi RESULTSET;
email varchar default 'drogaieva@gmail.com';
subject varchar default 'Policy Trn Load Complete';
BEGIN
  rs := (
select table_name
from mytest_db.information_schema.tables 
where table_type = 'BASE TABLE' 
and table_schema='POLICY_TRN'
and (table_name like 'DIM%' or table_name like 'FACT%' or table_name like '%AGG%')
and table_name<>'DIM_TRANSACTION'
order by  table_name
        );
  
  FOR record IN rs DO
  
   table_name := record.table_name;

   query := 
   'with ll as (select max(loaddate) d, count(*) t from mytest_db.policy_trn.' || table_name || ')' ||
   'select  ''<tr> ' ||
   ' <td>'' || ''' || table_name  ||  ''' || ''</td> ' || 
   ' <td>'' || count(*)   || ''</td> ' || 
   ' <td>'' || ll.d       || ''</td> ' || 
   ' <td>'' || ll.t       || ''</td> ' || 
   ' </tr>'' as d ' ||
   ' from mytest_db.policy_trn.' || table_name || ' trg ' ||
   ' join ll ' ||
   'on 1=1 ' ||
   ' where trg.loaddate = ll.d ' ||
   ' group by ll.d, ll.t;';


    rsi := (EXECUTE IMMEDIATE query);
    FOR record IN rsi DO
     data := data || record.d;
   END FOR;
 
  END FOR;
  
  data := data || '</TABLE>';

CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION('{ "text/html": "' || :data || '"}','{"my_email_int": {"subject": "' || :subject ||'","toAddress": ["' || :email ||'"]}}');

return data;
END;


