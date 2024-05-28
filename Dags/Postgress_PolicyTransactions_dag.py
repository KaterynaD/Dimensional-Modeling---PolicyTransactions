#-----------------------------------------------IMPORT LIBRARIES--------------------------------------
from airflow import DAG
from airflow.utils.dates import days_ago
#
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email_operator import EmailOperator
#
from airflow.decorators import task_group
#from airflow.utils.edgemodifier import Label
from airflow.models.baseoperator import chain
#
from airflow.models import Variable
from airflow.models import Connection
from airflow import settings
#
import os
import yaml
import json
from datetime import datetime
#-----------------------------------------------GLOBAL VARIABLES--------------------------------------
HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
dbt_project='dbt_postgres_policytransactions'
dbt_project_home = Variable.get('dbt_policy_trn')
dbt_path = os.path.join(HOME,'dags', dbt_project_home)
manifest_path = os.path.join(dbt_path, "target/manifest.json") # path to manifest.json

#path to SQL files used in the orchestration
LatestLoadedTransactionDate_sql_path = os.path.join(dbt_path, f"target/compiled/{dbt_project}/analyses/LatestLoadedTransactionDate.sql")
NewTransactionDate_sql_path = os.path.join(dbt_path, f"target/compiled/{dbt_project}/analyses/NewTransactionDate.sql")
daily_load_sql_path = os.path.join(dbt_path, f"target/compiled/{dbt_project}/analyses/daily_load.sql")



with open(manifest_path) as f: # Open manifest.json
        manifest = json.load(f) # Load its contents into a Python Dictionary
        nodes = manifest["nodes"] # Extract just the nodes

dbt_conn_id = 'dbt_policy_trn_connection'
#-----------------------------------------------PROCEDURES--------------------------------------
#-----------------------------------------------------------------------------------------------
def SetLoadDate(ti):
    #I need the variable with Loaddate
    ti.xcom_push(key='LoadDate',value=format(datetime.today().replace(microsecond=0)))
#-----------------------------------------------------------------------------------------------
def SyncConnection():
    with open(os.path.join(dbt_path,'profiles.yml')) as stream:
        try:  
            profile=yaml.safe_load(stream)[dbt_project]['outputs']['dev'] #can have a var to switch between dev and prod the same in this project

            conn_type=profile['type']
            username=profile['user']
            password=profile['pass']            
            host=profile['host']
            port=profile['port']
            db=profile['dbname']

            session = settings.Session()           

            try:
                
                new_conn = session.query(Connection).filter(Connection.conn_id == dbt_conn_id).one()
                new_conn.conn_type = conn_type
                new_conn.login = username
                new_conn.password = password
                new_conn.host = host
                new_conn.port = port
                new_conn.schema = db                     

            except:

                new_conn = Connection(conn_id=dbt_conn_id,
                                  conn_type=conn_type,
                                  login=username,
                                  password=password,
                                  host=host,
                                  port=port,
                                  schema=db)   

            
            session.add(new_conn)
            session.commit()    

        
        except yaml.YAMLError as exc:
            print(exc)
#-----------------------------------------------------------------------------------------------

def branch_Clear_DW_or_not():

    if Variable.get("dbt_policy_trn_clear_dw") == 'Y':
        return "Clear_DW.Full_Refresh_DW"
    else:
        return "Clear_DW.Do_not_Clear_DW_log"
    
#-----------------------------------------------------------------------------------------------

def branch_Is_Stgaging_Ready():

    if Variable.get("dbt_policy_trn_validate_stg") == 'Y':
        return "Staging_Validation.Validate_Stg"    
    else:
        return "Staging_Validation.Do_not_Validate_Stg_log"
#-----------------------------------------------------------------------------------------------

def branch_Load_Stg_or_not():

    if Variable.get("dbt_policy_trn_load_stg") == 'Y':
        return "Staging_Load.Load_Stg.Start_Stg_Load_log"
    else:
        return "Staging_Load.Do_not_Load_Stg_log"
#-----------------------------------------------------------------------------------------------

def dbt_nodes(node_tags, stg_Start_Node):
    # Create a dict of Operators
    dbt_tasks = dict()
    dbt_tasks[stg_Start_Node]=EmptyOperator(task_id=stg_Start_Node)
    for node_id, node_info in  nodes.items():
        if ((node_info["resource_type"]!="seed") &
            (
            (any(x in list(node_info["tags"]) for x in node_tags)) 
         & ~("scd2" in node_info["tags"])
            )
            ):     
            task_id=".".join(
                    [
                        node_info["resource_type"],
                        node_info["package_name"],
                        node_info["name"],
                    ]
                                    )      
            dbt_tasks[node_id] = BashOperator(
                        task_id=task_id,
                        bash_command='cd %s && dbt run --select %s --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%(dbt_path,node_info["name"]),
                        dag=dag)         



    # Define relationships between Operators only inside the same "tag"
    # Outside relationships are managed by the workflow
    for node_id, node_info in nodes.items():
        upstream_nodes = set()
        if ((node_info["resource_type"]!="seed") &
            (
            (any(x in list(node_info["tags"]) for x in node_tags)) 
         & ~("scd2" in node_info["tags"])
            )
            ): 
            for upstream_node in node_info["depends_on"]["nodes"]:                
                upstream_nodes.add(stg_Start_Node)
                if not any(value in upstream_node for value in ("source", "log", "seed")):                                
                    upstream_nodes.add(upstream_node)
        
                if upstream_nodes:
                    for upstream_node in list(upstream_nodes):
                        if upstream_node in dbt_tasks:
                            dbt_tasks[upstream_node] >> dbt_tasks[node_id]

    return dbt_tasks[stg_Start_Node]
#-----------------------------------------------------------------------------------------------

def branch_Load_Dims_or_not():

    if Variable.get("dbt_policy_trn_load_dims") == 'Y':
        return "Dims_Load.Load_Dim.Start_Dim_Load_log"
    else:
        return "Dims_Load.Do_not_Load_Dims_log"   
#-----------------------------------------------------------------------------------------------

def branch_Load_Facts_or_not():

    if Variable.get("dbt_policy_trn_load_facts") == 'Y':
        return "Facts_Load.Load_Fact.Start_Fact_Load_log"
    else:
        return "Facts_Load.Do_not_Load_Facts_log"   
#-----------------------------------------------------------------------------------------------

def branch_Load_Agg_or_not():

    if Variable.get("dbt_policy_trn_load_agg") == 'Y':
        return "Agg_Load.Load_Agg"
    else:
        return "Agg_Load.Do_not_Load_Agg_log"   
#-----------------------------------------------------------------------------------------------

def branch_Test_Load_or_not():

    if Variable.get("dbt_policy_trn_test_load") == 'Y':
        return "Test_Load.Load_Test"
    else:
        return "Test_Load.Do_not_Load_Test_log"    
#-----------------------------------------------------------------------------------------------

def branch_Refresh_Dashboards_or_not():

    if Variable.get("dbt_policy_trn_refresh_dashboards") == 'Y':
        return "Refresh_Dashboards.Refresh_Dashboards"    
    else:
        return "Refresh_Dashboards.Do_not_Refresh_Dashboards_log"        
#-----------------------------------------------------------------------------------------------

def LatestLoadedTransactionDate(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    try:
        with open(LatestLoadedTransactionDate_sql_path) as f: # Open sql file
            sql = f.read()
            records = pg_hook.get_records(sql) 
            if len(records)>0:
                for record in records:                   
                    ti.xcom_push(key='LatestLoadedTransactionDate',value=f'{record[0]}')
                    return record[0]   
            else:
                ti.xcom_push(key='LatestLoadedTransactionDate',value='19000101')
                return '19000101'                  
    except:
        ti.xcom_push(key='LatestLoadedTransactionDate',value='19000101')
        return '19000101'  
    #ti.xcom_push(key='LatestLoadedTransactionDate',value='19000101')
    #return '19000101' 
#-----------------------------------------------------------------------------------------------
def NewTransactionDate(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    with open(NewTransactionDate_sql_path) as f: # Open sql file
        sql = f.read()
        records = pg_hook.get_records(sql) 
        if len(records)>0:   
            for record in records:    
                ti.xcom_push(key='NewTransactionDate',value=f'{record[0]}')
                return record[0] 
        else:
            ti.xcom_push(key='NewTransactionDate',value=f'19000101')
            return '19000101'     
#-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------
def LoadCompleteEmail():
    email_body="<H2>Today's data:</H2>"
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    with open(daily_load_sql_path) as f: # Open sql file
        sql = f.read()
        records = pg_hook.get_records(sql)  
        email_body += "<table border=1>"  
        email_body += ("<tr><th>Table Name</th>" + 
        "<th>Count Records on LoadDate</th>" +
        "<th>LoadDate</th>" +
        "<th>Total Records</th>" +
        "<th>Latest Load on</th></tr>")
        for record in records:  
            email_body += ("<tr><td>" + str(record[0]) + "</td>" + 
            "<td>" + str(record[1]) + "</td>"
            "<td>" + str(record[2]) + "</td>"
            "<td>" + str(record[3]) + "</td>"
            "<td>" + str(record[4]) + "</td></tr>")
        email_body += "</table>"
    return email_body


#-----------------------------------------------------------------------------------------------
def SetEndLoadDate(ti):
    #I need the variable with Loaddate
    ti.xcom_push(key='EndLoadDate',value=format(datetime.today().replace(microsecond=0)))

#-----------------------------------------------------------------------------------------------
def report_failure(context):
    # include this check if you only want to get one email per DAG
    #if(task_instance.xcom_pull(task_ids=None, dag_id=dag_id, key=dag_id) == True):
    #    logging.info("Other failing task has been notified.")
    send_email = EmailOperator(to=Variable.get("send_alert_to"),
            subject=dbt_project + 'Load Failed'
            )
    send_email.execute(context)
#----------------------------------------------- DAG --------------------------------------------
args = {
    'owner': 'airflow',
    'on_failure_callback': report_failure ,
    'email': Variable.get("send_alert_to"),
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'start_date': days_ago(0)
}


with DAG(dag_id="Postgress_PolicyTransactions_dag",default_args=args,
    schedule_interval='0 23 * * *',
    catchup=False) as dag:
#-----------------------------------------------START LOAD--------------------------------------

    @task_group(group_id="Start_Load",prefix_group_id=True,tooltip="This task group initiate loadd date, sync DBT and Airflow connections",)
    def Start_Load():
        Set_Load_Date = PythonOperator(
            task_id="Set_Load_Date",
            python_callable=SetLoadDate,
            provide_context=True)

        Sync_Connection = PythonOperator(
            task_id="Sync_Connection",
            python_callable=SyncConnection)

        
        Set_Load_Date >> Sync_Connection

    #-----------------------------------------------Clear DW--------------------------------------
    @task_group(group_id="Clear_DW",prefix_group_id=True,tooltip="It's needed to start from scratch - empty tables or load history")
    def Clear_DW():
        Clear_DW_or_not = BranchPythonOperator(
        task_id="Clear_DW_or_not",
        provide_context=True,
        python_callable=branch_Clear_DW_or_not,
        dag=dag,
        )

        Full_Refresh_DW = BashOperator(
        task_id="Full_Refresh_DW",
        bash_command='cd %s && dbt run --full-refresh --vars \'{"load_defaults":{{ var.value.dbt_policy_trn_load_defaults }}, "loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}","latest_loaded_transactiondate":"19000101","new_transactiondate":"19000101"}\''%dbt_path
        )

        Seed_DW = BashOperator(
        task_id="Seed_DW",
        bash_command='cd %s && dbt seed'%dbt_path
        )        
    
        Do_not_Clear_DW_log = EmptyOperator(task_id="Do_not_Clear_DW_log")

        Clear_DW_or_not >> [Full_Refresh_DW, Do_not_Clear_DW_log]
        Full_Refresh_DW >> Seed_DW
    #-----------------------------------------------INCREMENTAL LOAD RANGE--------------------------------------    
    @task_group(group_id="Incremental_Load_Range",prefix_group_id=True,tooltip="This task group initiates transaction dates range to load and starts a new entry in the orchestration_log",)
    def Incremental_Load_Range():
        Compile_Latest_Loaded_TransactionDate_sql = BashOperator(
        task_id="Compile_Latest_Loaded_TransactionDate_sql",
        bash_command='cd %s && dbt compile  --select LatestLoadedTransactionDate --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}"}\''%dbt_path,
        trigger_rule="one_success"
        )        

        Latest_Loaded_TransactionDate = PythonOperator(
            task_id='Latest_Loaded_TransactionDate',
            python_callable=LatestLoadedTransactionDate,
            provide_context=True)
        
        Compile_New_TransactionDate_sql = BashOperator(
        task_id="Compile_New_TransactionDate_sql",
        bash_command='cd %s && dbt compile  --select NewTransactionDate --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}"}\''%dbt_path
        )             

        New_TransactionDate = PythonOperator(
            task_id='New_TransactionDate',
            python_callable=NewTransactionDate,
            provide_context=True)

        Init_Orchestration_Log = BashOperator(
            task_id='Init_Orchestration_Log',
            bash_command='cd %s && dbt run --select orchestration_log --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path
            )

        Compile_Latest_Loaded_TransactionDate_sql >> Latest_Loaded_TransactionDate >> Compile_New_TransactionDate_sql >> New_TransactionDate >> Init_Orchestration_Log

    #-----------------------------------------------STAGING VALIDATION--------------------------------------
    @task_group(group_id="Staging_Validation",prefix_group_id=True,tooltip="Dummy Task. It might check S3 or End of Day Complete in the source system or if any new data present. Not a part of DBT workflow.")
    def Staging_Validation():
        Is_Stgaging_Ready = BranchPythonOperator(
        task_id="Is_Stgaging_Ready",
        provide_context=True,
        python_callable=branch_Is_Stgaging_Ready,
        dag=dag,
        trigger_rule="one_success"
        )

        Validate_Stg = EmptyOperator(task_id="Validate_Stg",)
        
    
        Do_not_Validate_Stg_log = EmptyOperator(task_id="Do_not_Validate_Stg_log")  
                

        Is_Stgaging_Ready >> [Validate_Stg, Do_not_Validate_Stg_log]

    #-----------------------------------------------LOAD STAGING--------------------------------------
    @task_group(group_id="Staging_Load",prefix_group_id=True,tooltip="If any additional transformtions are needed to shape staging data - a connect/query/export-import/load-to-from-S3 backet")
    def Staging_Load():        
        Load_Stg_or_not = BranchPythonOperator(
        task_id="Load_Stg_or_not",
        provide_context=True,
        python_callable=branch_Load_Stg_or_not,
        dag=dag,
        trigger_rule="one_success"
        )

        @task_group(group_id="Load_Stg",prefix_group_id=True,tooltip="Dummy Task. If there is no tool like FiveTran, a connect/query/export-import/load-to-from-S3 backet may needed")
        def Load_Stg():
            dbt_nodes(["stg"], "Start_Stg_Load_log")
  
        Do_not_Load_Stg = EmptyOperator(task_id="Do_not_Load_Stg_log") 

        Load_Stg_or_not >> [Load_Stg(), Do_not_Load_Stg]

    #-----------------------------------------------LOAD DIMENSIONS------------------------------------------
    @task_group(group_id="Dims_Load",prefix_group_id=True,tooltip="Load dimensions using run models and/or macros")
    def Dims_Load():
        Load_Dims_or_not = BranchPythonOperator(
        task_id="Load_Dims_or_not",
        provide_context=True,
        python_callable=branch_Load_Dims_or_not,
        dag=dag,
        trigger_rule="one_success"
        )

        @task_group(group_id="Load_Dim",prefix_group_id=True,tooltip="Load Dimensions")
        def Load_Dim():

            Start_Dim_Load_log = dbt_nodes(["dim"], "Start_Dim_Load_log") 
                    
            Load_scd2_dim_vehicle = BashOperator(
                        task_id="Load_scd2_dim_vehicle",
                        bash_command='cd %s && dbt run-operation load_scd2 --args \'{"dim_table_name":"dim_vehicle","stg_data":"stg_vehicle","dim_primary_key":"vehicle_id","dim_unique_key":"vehicle_uniqueid","dim_change_date":"transactioneffectivedate","history_tracking_columns":["vin", "model", "modelyr", "manufacturer", "estimatedannualdistance"]}\' --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
                        dag=dag)   

            Load_scd2_dim_driver = BashOperator(
                        task_id="Load_scd2_dim_driver",
                        bash_command='cd %s && dbt run-operation load_scd2 --args \'{"dim_table_name":"dim_driver","stg_data":"stg_driver","dim_primary_key":"driver_id","dim_unique_key":"driver_uniqueid","dim_change_date":"transactioneffectivedate","history_tracking_columns":["gendercd", "birthdate", "maritalstatuscd", "pointscharged"]}\' --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
                        dag=dag)               

            Start_Dim_Load_log >> Load_scd2_dim_vehicle 
            Start_Dim_Load_log >> Load_scd2_dim_driver             

        Do_not_Load_Dims = EmptyOperator(task_id="Do_not_Load_Dims_log")  

        Load_Dims_or_not >> [Load_Dim() , Do_not_Load_Dims]



    #-----------------------------------------------LOAD FACTS--------------------------------------
    @task_group(group_id="Facts_Load",prefix_group_id=True,tooltip="Load fact tables using run models and/or macros")
    def Facts_Load():
        Load_Facts_or_not = BranchPythonOperator(
        task_id="Load_Facts_or_not",
        provide_context=True,
        python_callable=branch_Load_Facts_or_not,
        dag=dag,
        trigger_rule="one_success"
        )

        @task_group(group_id="Load_Fact",prefix_group_id=True,tooltip="Load Dimensions")
        def Load_Fact():
            dbt_nodes(["fact"], "Start_Fact_Load_log")
    
        Do_not_Load_Facts = EmptyOperator(task_id="Do_not_Load_Facts_log")   

        Load_Facts_or_not >> [Load_Fact(), Do_not_Load_Facts]  

    #-----------------------------------------------LOAD AGGREGATIONS---------------------------------
    @task_group(group_id="Agg_Load",prefix_group_id=True,tooltip="Load fact tables with complex aggregations or other complex transformations for reporting using run models and/or macros")
    def Agg_Load():
        Load_Agg_or_not = BranchPythonOperator(
        task_id="Load_Agg_or_not",
        provide_context=True,
        python_callable=branch_Load_Agg_or_not,
        dag=dag,
        trigger_rule="one_success"
        )
        
        Load_Agg = BashOperator(
                        task_id="Load_Agg",
                        bash_command='cd %s && dbt run-operation load_dummy_agg_table --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
                        dag=dag)   
    
        Do_not_Load_Agg_log = EmptyOperator(task_id="Do_not_Load_Agg_log")           
    
        Load_Agg_or_not >> [Load_Agg, Do_not_Load_Agg_log]
    #-----------------------------------------------END LOAD--------------------------------------
    @task_group(group_id="End_Load",prefix_group_id=True,tooltip="This task group finalizes load with update active record in etl_log",)
    def End_Load():
        Set_End_Load_Date = PythonOperator(
        task_id="Set_End_Load_Date",
        python_callable=SetEndLoadDate,
        trigger_rule="one_success",
        provide_context=True)

        Complete_ETL_run_Log = BashOperator(
        task_id='Complete_ETL_run_Log',
        bash_command='cd %s && dbt run-operation orchestration_log_update --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "endloaddate":"{{ti.xcom_pull(task_ids=\'End_Load.Set_End_Load_Date\', key=\'EndLoadDate\')}}"}\''%dbt_path)

        Compile_daily_load_sql = BashOperator(
        task_id="Compile_daily_load_sql",
        bash_command='cd %s && dbt compile  --select daily_load --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}"}\''%dbt_path
        )

        Load_Complete_Email = EmailOperator(
        task_id='Load_Complete_Email',
        to="{{var.value.get('send_alert_to')}}",
        subject=dbt_project + ": Load Complete",
        html_content=LoadCompleteEmail(),
        dag=dag
)
        Set_End_Load_Date >> Complete_ETL_run_Log >> Compile_daily_load_sql >> Load_Complete_Email

    #-----------------------------------------------TEST LOAD---------------------------------
    @task_group(group_id="Test_Load",prefix_group_id=True,tooltip="Test loaded data")
    def Test_Load():
        Test_Load_or_not = BranchPythonOperator(
        task_id="Test_Load_or_not",
        provide_context=True,
        python_callable=branch_Test_Load_or_not,
        dag=dag,
        trigger_rule="one_success"
        )

        Load_Test = BashOperator(
        task_id="Load_Test",
        bash_command='cd %s && dbt test --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}","latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}","new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Range.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
        )
    
        Do_not_Load_Test_log = EmptyOperator(task_id="Do_not_Load_Test_log")           
    
        Test_Load_or_not >> [Load_Test, Do_not_Load_Test_log]

#-----------------------------------------------REFRESH DASHBOARDS---------------------------------
    @task_group(group_id="Refresh_Dashboards",prefix_group_id=True,tooltip="Test loaded data")
    def Refresh_Dashboards():
        Refresh_Dashboards_or_not = BranchPythonOperator(
        task_id="Refresh_Dashboards_or_not",
        provide_context=True,
        python_callable=branch_Refresh_Dashboards_or_not,
        dag=dag,
        trigger_rule="one_success"
        )

        Refresh_Dashboards = EmptyOperator(task_id="Refresh_Dashboards") 
    
        Do_not_Refresh_Dashboards_log = EmptyOperator(task_id="Do_not_Refresh_Dashboards_log")           
    
        Refresh_Dashboards_or_not >> [Refresh_Dashboards, Do_not_Refresh_Dashboards_log]        

    #-----------------------------------------------CHAIN of TASKS--------------------------------------
    chain(
     Start_Load(),
     #
     Clear_DW(),
     #
     Incremental_Load_Range(),
     #
     Staging_Validation(),
     #
     Staging_Load(),
     #
     Dims_Load(),
     #
     Facts_Load(),
     #
     Agg_Load(),
     #
     End_Load(),
     #
     Test_Load(),
     #
     Refresh_Dashboards()
    )