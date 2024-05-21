#-----------------------------------------------IMPORT LIBRARIES--------------------------------------
from airflow import DAG
from airflow.utils.dates import days_ago
#
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
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
from datetime import datetime
#-----------------------------------------------GLOBAL VARIABLES--------------------------------------
HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
dbt_project='dbt_postgres_policytransactions'
dbt_project_home = Variable.get('dbt_policy_trn')
dbt_path = os.path.join(HOME,'dags', dbt_project_home)
dbt_conn_id = 'dbt_policy_trn_connection'
#-----------------------------------------------PROCEDURES--------------------------------------
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
def LatestLoadedTransactionDate(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    sql = "SELECT transactiondate FROM policy_trn.etl_log WHERE LoadDate=(select max(loaddate) from policy_trn.etl_log where endloaddate is not null)"
    try:
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
    LatestLoadedTransactionDate=ti.xcom_pull(task_ids='start_load.Latest_Loaded_TransactionDate', key='LatestLoadedTransactionDate')
    sql = f"SELECT min(transactiondate) transactiondate FROM policy_trn.etl_history WHERE transactiondate>{LatestLoadedTransactionDate}"
    records = pg_hook.get_records(sql) 
    for record in records:    
        ti.xcom_push(key='NewTransactionDate',value=f'{record[0]}')
        return record[0] 
#-----------------------------------------------------------------------------------------------
def GetNumbatchesStg1(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    sql = "SELECT num_batches FROM policy_trn.etl_stg1_scd2_num_batches WHERE table_name='stg_risk'"
    records = pg_hook.get_records(sql) 
    for record in records:    
        ti.xcom_push(key='Numbatches_stg1',value=f'{record[0]}')
        return record[0]  
#-----------------------------------------------------------------------------------------------    
def GetNumbatchesStg2(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    sql = "SELECT num_batches FROM policy_trn.etl_stg2_scd2_num_batches WHERE table_name='dim_risk'"
    records = pg_hook.get_records(sql) 
    for record in records:    
        ti.xcom_push(key='Numbatches_stg2',value=f'{record[0]}')
        return record[0]     
#-----------------------------------------------------------------------------------------------
def SetEndLoadDate(ti):
    #I need the variable with Loaddate
    ti.xcom_push(key='EndLoadDate',value=format(datetime.today().replace(microsecond=0)))

#----------------------------------------------- DAG --------------------------------------------



with DAG(dag_id="dbt_scd2_dag",default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False) as dag:
#-----------------------------------------------START LOAD--------------------------------------

    @task_group(group_id="start_load",prefix_group_id=True,tooltip="This task group initiate loadd date, transaction date to load set of transactions up to teh date and etl_log",)
    def start_load():
        Set_Load_Date = PythonOperator(
        task_id="Set_Load_Date",
        python_callable=SetLoadDate,
        provide_context=True)

        Sync_Connection = PythonOperator(
        task_id="Sync_Connection",
        python_callable=SyncConnection)

        Latest_Loaded_TransactionDate = PythonOperator(
        task_id='Latest_Loaded_TransactionDate',
        python_callable=LatestLoadedTransactionDate,
        provide_context=True)

        New_TransactionDate = PythonOperator(
        task_id='New_TransactionDate',
        python_callable=NewTransactionDate,
        provide_context=True)

        Init_ETL_run_Log = BashOperator(
        task_id='Init_ETL_run_Log',
        bash_command='cd %s && dbt run --select etl_log --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path
        )
        Set_Load_Date >> Sync_Connection >> Latest_Loaded_TransactionDate >> New_TransactionDate >> Init_ETL_run_Log
#-----------------------------------------------STG_RISK--------------------------------------
    @task_group(group_id="stg_risk",prefix_group_id=True,tooltip="This task group load data in stg_risk",)
    def stg_risk():
        ETL_stg1_scd2_num_batches = BashOperator(
        task_id='ETL_stg1_scd2_num_batches',
        bash_command='cd %s && dbt run --select etl_stg1_scd2_num_batches --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}",  "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}",   "new_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path)


        Get_Numbatches_stg1 = PythonOperator(
        task_id='Get_Numbatches_stg1',
        python_callable=GetNumbatchesStg1,
        provide_context=True)


        Load_stg_risk = BashOperator(
        task_id='Load_stg_risk',
        bash_command='cd %s && for i in `seq 1 {{ti.xcom_pull(task_ids=\'stg_risk.Get_Numbatches_stg1\', key=\'Numbatches_stg1\')}}`; do dbt snapshot --select stg_risk --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}",  "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}",   "new_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.New_TransactionDate\', key=\'NewTransactionDate\')}}", "batch_num":"\'$i\'"}\'; done'%dbt_path)

        ETL_stg1_scd2_num_batches >> Get_Numbatches_stg1 >> Load_stg_risk
    #-----------------------------------------------DIM_RISK--------------------------------------

    @task_group(group_id="dim_risk",prefix_group_id=True,tooltip="This task group load data in dim_risk",)
    def dim_risk():
        ETL_stg2_scd2_num_batches = BashOperator(
        task_id='ETL_stg2_scd2_num_batches',
        bash_command='cd %s && dbt run --select etl_stg2_scd2_num_batches --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}",  "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}",   "new_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path)

        Get_Numbatches_stg2 = PythonOperator(
        task_id='Get_Numbatches_stg2',
        python_callable=GetNumbatchesStg2,
        provide_context=True)


        Load_dim_risk = BashOperator(
        task_id='Load_dim_risk',
        bash_command='cd %s && for i in `seq 1 {{ti.xcom_pull(task_ids=\'dim_risk.Get_Numbatches_stg2\', key=\'Numbatches_stg2\')}}`; do dbt snapshot --select dim_risk --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}",  "latest_loaded_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.Latest_Loaded_TransactionDate\', key=\'LatestLoadedTransactionDate\')}}",   "new_transactiondate":"{{ti.xcom_pull(task_ids=\'start_load.New_TransactionDate\', key=\'NewTransactionDate\')}}", "batch_num":"\'$i\'"}\'; done'%dbt_path)
        #-----------------------------------------------LATER COMING DIMENSIONAL ATTRIBUTES-----------
        Back_Dated_SCD2_Records_Insert = BashOperator(
        task_id='Back_Dated_SCD2_Records_Insert',
        bash_command='cd %s && dbt run-operation back_dated_scd2_records_insert --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}"}\''%dbt_path)

        ETL_stg2_scd2_num_batches >> Get_Numbatches_stg2 >> Load_dim_risk >> Back_Dated_SCD2_Records_Insert
    #-----------------------------------------------END LOAD--------------------------------------
    @task_group(group_id="end_load",prefix_group_id=True,tooltip="This task group finalizes load with update active record in etl_log",)
    def end_load():
        Set_End_Load_Date = PythonOperator(
        task_id="Set_End_Load_Date",
        python_callable=SetEndLoadDate,
        provide_context=True)

        Complete_ETL_run_Log = BashOperator(
        task_id='Complete_ETL_run_Log',
        bash_command='cd %s && dbt run-operation etl_log_update --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'start_load.Set_Load_Date\', key=\'LoadDate\')}}", "endloaddate":"{{ti.xcom_pull(task_ids=\'end_load.Set_End_Load_Date\', key=\'EndLoadDate\')}}"}\''%dbt_path)

        Set_End_Load_Date >> Complete_ETL_run_Log
    #-----------------------------------------------CHAIN of TASKS--------------------------------------
    chain(
     start_load(),
     stg_risk(),
     dim_risk(),
     end_load() )