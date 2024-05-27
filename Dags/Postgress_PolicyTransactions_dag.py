#-----------------------------------------------IMPORT LIBRARIES--------------------------------------
from airflow import DAG
from airflow.utils.dates import days_ago
#
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
#
from airflow.decorators import task_group
from airflow.utils.edgemodifier import Label
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
LatestLoadedTransactionDate_sql_path = os.path.join(dbt_path, "target/compiled/{dbt_project}/analysis/LatestLoadedTransactionDate.sql")
NewTransactionDate_sql_path = os.path.join(dbt_path, "target/compiled/{dbt_project}/analysis/NewTransactionDate.sql")


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

def branch_Clear_DW_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_clear_dw')

    if airflow_variable_value == 'True':
        return "Clear_DW"
    
#-----------------------------------------------------------------------------------------------

def branch_Is_Stgaging_Ready(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_validate_stg')

    if airflow_variable_value == 'True':
        return "Validate_Stg"    

#-----------------------------------------------------------------------------------------------

def branch_Load_Stg_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_load_stg')

    if airflow_variable_value == 'True':
        return "Load_Stg"

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
                        bash_command='cd %s && dbt run --select %s --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}, "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%(dbt_path,node_info["name"]),
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

def branch_Load_Dims_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_load_dims')

    if airflow_variable_value == 'True':
        return "Load_Dims"
    
#-----------------------------------------------------------------------------------------------

def branch_Load_Facts_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_load_facts')

    if airflow_variable_value == 'True':
        return "Load_Facts"
    
#-----------------------------------------------------------------------------------------------

def branch_Load_Agg_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_load_agg')

    if airflow_variable_value == 'True':
        return "Load_Agg"
    
#-----------------------------------------------------------------------------------------------

def branch_Test_Load_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_test_load')

    if airflow_variable_value == 'True':
        return "Test_Load"
    
#-----------------------------------------------------------------------------------------------

def branch_Refresh_Dashboards_or_not(**kwargs):
    airflow_variable_value = kwargs['dag_run'].conf.get('dbt_policy_trn_refresh_dashboards')

    if airflow_variable_value == 'True':
        return "Refresh_Dashboards"    
        
#-----------------------------------------------------------------------------------------------

def LatestLoadedTransactionDate(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    try:
        with open(LatestLoadedTransactionDate_sql_path) as f: # Open sql file
            sql = f.read()
            records = pg_hook.get_records(sql) 
            for record in records:    
                ti.xcom_push(key='LatestLoadedTransactionDate',value=f'{record[0]}')
                return record[0]    
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
        for record in records:    
            ti.xcom_push(key='NewTransactionDate',value=f'{record[0]}')
            return record[0] 
#-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------
def SetEndLoadDate(ti):
    #I need the variable with Loaddate
    ti.xcom_push(key='EndLoadDate',value=format(datetime.today().replace(microsecond=0)))

#----------------------------------------------- DAG --------------------------------------------



with DAG(dag_id="Postgress_PolicyTransactions_dag",default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False) as dag:
#-----------------------------------------------START LOAD--------------------------------------

    @task_group(group_id="Start_Load",prefix_group_id=True,tooltip="This task group initiate loadd date, sync SBT and Airflow connections",)
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
        Clear_DW_or_not = PythonOperator(
        task_id="Clear_DW_or_not",
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
    
        Do_not_Clear_Dw = EmptyOperator(task_id="Do_not_Clear_Dw")

        Clear_DW_or_not >> [Full_Refresh_DW, Do_not_Clear_Dw]
        Full_Refresh_DW >> Seed_DW
    #-----------------------------------------------INCREMENTAL LOAD RANGE--------------------------------------    
    @task_group(group_id="Incremental_Load_Range",prefix_group_id=True,tooltip="This task group initiates transaction dates range to load and starts a new entry in the orchestration_log",)
    def Incremental_Load_Range():

        Compile_Latest_Loaded_TransactionDate_sql = BashOperator(
        task_id="Compile_Latest_Loaded_TransactionDate_sql",
        bash_command='cd %s && dbt compile  --select LatestLoadedTransactionDate --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}"}\''%dbt_path
        )        

        Latest_Loaded_TransactionDate = PythonOperator(
            task_id='Latest_Loaded_TransactionDate',
            python_callable=LatestLoadedTransactionDate,
            provide_context=True)
        
        Compile_New_TransactionDate_sql = BashOperator(
        task_id="Compile_New_TransactionDate_sql",
        bash_command='cd %s && dbt compile  --select NewTransactionDate --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}"}\''%dbt_path
        )             

        New_TransactionDate = PythonOperator(
            task_id='New_TransactionDate',
            python_callable=NewTransactionDate,
            provide_context=True)

        Init_Orchestration_Log = BashOperator(
            task_id='Init_Orchestration_Log',
            bash_command='cd %s && dbt run --select orchestration_log --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path
            )

        Compile_Latest_Loaded_TransactionDate_sql >> Latest_Loaded_TransactionDate >> Compile_New_TransactionDate_sql >> New_TransactionDate >> Init_Orchestration_Log

    #-----------------------------------------------STAGING VALIDATION--------------------------------------
    @task_group(group_id="Staging_Validation",prefix_group_id=True,tooltip="Dummy Task. It might check S3 or End of Day Complete in the source system or if any new data present. Not a part of DBT workflow.")
    def Staging_Validation():
        Is_Stgaging_Ready = PythonOperator(
        task_id="Is_Stgaging_Ready",
        python_callable=branch_Is_Stgaging_Ready,
        dag=dag,
        )

        Validate_Stg = EmptyOperator(task_id="Validate_Stg",)
    
        Do_not_Validate_Stg = EmptyOperator(task_id="Do_not_Validate_Stg")  

        Is_Stgaging_Ready >> [Validate_Stg, Do_not_Validate_Stg]

    #-----------------------------------------------LOAD STAGING--------------------------------------
    @task_group(group_id="Staging_Load",prefix_group_id=True,tooltip="If any additional transformtions are needed to shape staging data - a connect/query/export-import/load-to-from-S3 backet")
    def Staging_Load():
        Load_Stg_or_not = PythonOperator(
        task_id="Load_Stg_or_not",
        python_callable=branch_Load_Stg_or_not,
        dag=dag,
        )

        @task_group(group_id="Load_Stg",prefix_group_id=True,tooltip="Dummy Task. If there is no tool like FiveTran, a connect/query/export-import/load-to-from-S3 backet may needed")
        def Load_Stg():
            dbt_nodes(["stg"], "Start_Stg_Load_log")
  
        Do_not_Load_Stg = EmptyOperator(task_id="Do_not_Load_Stg_log") 

        Load_Stg_or_not >> [Load_Stg(), Do_not_Load_Stg]

    #-----------------------------------------------LOAD DIMENSIONS------------------------------------------
    @task_group(group_id="Dims_Load",prefix_group_id=True,tooltip="Load dimensions using run models and/or macros")
    def Dims_Load():
        Load_Dims_or_not = PythonOperator(
        task_id="Load_Dims_or_not",
        python_callable=branch_Load_Dims_or_not,
        dag=dag,
        )

        @task_group(group_id="Load_Dim",prefix_group_id=True,tooltip="Load Dimensions")
        def Load_Dim():

            Start_Dim_Load_log = dbt_nodes(["dim"], "Start_Dim_Load_log") 
                    
            Load_scd2_dim_vehicle = BashOperator(
                        task_id="Load_scd2_dim_vehicle",
                        bash_command='cd %s && dbt run-operation load_scd2 --args \'{"dim_table_name":"dim_vehicle","stg_data":"stg_vehicle","dim_primary_key":"vehicle_id","dim_unique_key":"vehicle_uniqueid","dim_change_date":"transactioneffectivedate","history_tracking_columns":["vin", "model", "modelyr", "manufacturer", "estimatedannualdistance"]}\' --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}, "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
                        dag=dag)   

            Load_scd2_dim_driver = BashOperator(
                        task_id="Load_scd2_dim_driver",
                        bash_command='cd %s && dbt run-operation load_scd2 --args \'{"dim_table_name":"dim_driver","stg_data":"stg_driver","dim_primary_key":"driver_id","dim_unique_key":"driver_uniqueid","dim_change_date":"transactioneffectivedate","history_tracking_columns":["gendercd", "birthdate", "maritalstatuscd", "pointscharged"]}\' --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}, "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
                        dag=dag)               

            Start_Dim_Load_log >> Load_scd2_dim_vehicle 
            Start_Dim_Load_log >> Load_scd2_dim_driver             

        Do_not_Load_Dims = EmptyOperator(task_id="Do_not_Load_Dims_log")  

        Load_Dims_or_not >> [Load_Dim() , Do_not_Load_Dims]



    #-----------------------------------------------LOAD FACTS--------------------------------------
    @task_group(group_id="Facts_Load",prefix_group_id=True,tooltip="Load fact tables using run models and/or macros")
    def Facts_Load():
        Load_Facts_or_not = PythonOperator(
        task_id="Load_Facts_or_not",
        python_callable=branch_Load_Facts_or_not,
        dag=dag,
        )

        @task_group(group_id="Load_Fact",prefix_group_id=True,tooltip="Load Dimensions")
        def Load_Fact():
            dbt_nodes(["fact"], "Start_Fact_Load_log")
    
        Do_not_Load_Facts = EmptyOperator(task_id="Do_not_Load_Facts_log")   

        Load_Facts_or_not >> [Load_Fact(), Do_not_Load_Facts]  

    #-----------------------------------------------LOAD AGGREGATIONS---------------------------------
    @task_group(group_id="Agg_Load",prefix_group_id=True,tooltip="Load fact tables with complex aggregations or other complex transformations for reporting using run models and/or macros")
    def Agg_Load():
        Load_Agg_or_not = PythonOperator(
        task_id="Load_Agg_or_not",
        python_callable=branch_Load_Agg_or_not,
        dag=dag,
        )
        
        Load_Agg = BashOperator(
                        task_id="Load_Agg",
                        bash_command='cd %s && dbt run-operation load_dummy_agg_table --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}, "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
                        dag=dag)   
    
        Do_not_Load_Agg = EmptyOperator(task_id="Do_not_Load_Agg")           
    
        Load_Agg_or_not >> [Load_Agg, Do_not_Load_Agg]
    #-----------------------------------------------END LOAD--------------------------------------
    @task_group(group_id="End_Load",prefix_group_id=True,tooltip="This task group finalizes load with update active record in etl_log",)
    def End_Load():
        Set_End_Load_Date = PythonOperator(
        task_id="Set_End_Load_Date",
        python_callable=SetEndLoadDate,
        provide_context=True)

        Complete_ETL_run_Log = BashOperator(
        task_id='Complete_ETL_run_Log',
        bash_command='cd %s && dbt run-operation orchestration_log_update --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}", "endloaddate":"{{ti.xcom_pull(task_ids=\'end_load.Set_End_Load_Date\', key=\'EndLoadDate\')}}"}\''%dbt_path)

        Set_End_Load_Date >> Complete_ETL_run_Log

    #-----------------------------------------------TEST LOAD---------------------------------
    @task_group(group_id="Test_Load",prefix_group_id=True,tooltip="Test loaded data")
    def Test_Load():
        Test_Load_or_not = PythonOperator(
        task_id="Test_Load_or_not",
        python_callable=branch_Test_Load_or_not,
        dag=dag,
        )

        Test_Load = BashOperator(
        task_id="Test_Load",
        bash_command='cd %s && dbt test --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Start_Load.Set_Load_Date\', key=\'LoadDate\')}}", "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}, "new_transactiondate":"{{ti.xcom_pull(task_ids=\'Incremental_Load_Rang.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,# run the selected model!
        )
    
        Do_not_Test_Load = EmptyOperator(task_id="Do_not_Test_Load")           
    
        Test_Load_or_not >> [Test_Load, Do_not_Test_Load]

#-----------------------------------------------REFRESH DASHBOARDS---------------------------------
    @task_group(group_id="Refresh_Dashboards",prefix_group_id=True,tooltip="Test loaded data")
    def Refresh_Dashboards():
        Refresh_Dashboards_or_not = PythonOperator(
        task_id="Refresh_Dashboards_or_not",
        python_callable=branch_Refresh_Dashboards_or_not,
        dag=dag,
        )

        Refresh_Dashboards = EmptyOperator(task_id="Refresh_Dashboards") 
    
        Do_not_Refresh_Dashboards = EmptyOperator(task_id="Do_not_Refresh_Dashboards")           
    
        Refresh_Dashboards_or_not >> [Refresh_Dashboards, Do_not_Refresh_Dashboards]        

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