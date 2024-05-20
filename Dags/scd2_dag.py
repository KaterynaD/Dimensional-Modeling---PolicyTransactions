from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.utils.edgemodifier import Label



from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow.models import Connection
from airflow import settings
import os
import yaml

from datetime import datetime

HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
dbt_project='dbt_postgres_policytransactions'
dbt_project_home = Variable.get('dbt_policy_trn')
dbt_path = os.path.join(HOME,'dags', dbt_project_home)
dbt_conn_id = 'dbt_policy_trn_connection'


def SetLoadDate(ti):
    #I need the variable with Loaddate
    ti.xcom_push(key='LoadDate',value=format(datetime.today().replace(microsecond=0)))

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



def LatestLoadedTransactionDate(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    sql = "SELECT transactiondate FROM policy_trn.etl_log WHERE LoadDate=(select max(loaddate) from policy_trn.etl_log where endloaddate is not null)"
    try:
        records = pg_hook.get_records(sql) 
        for record in records:    
            ti.xcom_push(key='LatestLoadedTransactionDate',value=f'{record[0]}')
            return record[0]    
    except:
        ti.xcom_push(key='LatestLoadedTransactionDate',value='1900-01-01')
        return '1900-01-01'  

    

def NewTransactionDate(ti):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    LatestLoadedTransactionDate=ti.xcom_pull(task_ids='Latest_Loaded_TransactionDate', key='LatestLoadedTransactionDate')
    sql = f"SELECT min(transactiondate) transactiondate FROM policy_trn.etl_history WHERE transactiondate>cast(to_char(to_date('{LatestLoadedTransactionDate}','yyyy-mm-dd'), 'yyyymmdd') as int)"
    records = pg_hook.get_records(sql) 
    for record in records:    
        ti.xcom_push(key='NewTransactionDate',value=f'{record[0]}')
        return record[0] 
    
def get_num_batches(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=dbt_conn_id)
    sql = "SELECT num_batches+42 FROM policy_trn.etl_stg1_scd2_num_batches WHERE table_name='stg_risk'"
    records = pg_hook.get_records(sql) 
    for record in records:    
        #ti.xcom_push(key='OHLC_full_filename',value=OHLC_full_filename)
        return record[0]  

def show_num_batches(**kwargs):
    ti = kwargs['ti']
    num_batches = ti.xcom_pull(task_ids='get_num_batches')
    print(num_batches)

def SetEndLoadDate(ti):
    #I need the variable with Loaddate
    ti.xcom_push(key='EndLoadDate',value=format(datetime.today().replace(microsecond=0)))

dag = DAG('dbt_scd2_dag', default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False)




Set_Load_Date = PythonOperator(
    task_id="Set_Load_Date",
    python_callable=SetLoadDate,
    provide_context=True,
    dag=dag
)

Sync_Connection = PythonOperator(
    task_id="Sync_Connection",
    python_callable=SyncConnection,
    dag=dag
)


Latest_Loaded_TransactionDate = PythonOperator(
    task_id='Latest_Loaded_TransactionDate',
    python_callable=LatestLoadedTransactionDate,
    provide_context=True,
    dag=dag)

New_TransactionDate = PythonOperator(
    task_id='New_TransactionDate',
    python_callable=NewTransactionDate,
    provide_context=True,
    dag=dag)


Init_ETL_run_Log = BashOperator(
    task_id='Init_ETL_run_Log',
    bash_command='cd %s && dbt run --select etl_log --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Set_Load_Date\', key=\'LoadDate\')}}", "new_transactiondate":"{{ti.xcom_pull(task_ids=\'New_TransactionDate\', key=\'NewTransactionDate\')}}"}\''%dbt_path,
    dag=dag
)

Set_End_Load_Date = PythonOperator(
    task_id="Set_End_Load_Date",
    python_callable=SetEndLoadDate,
    provide_context=True,
    dag=dag
)

Complete_ETL_run_Log = BashOperator(
    task_id='Complete_ETL_run_Log',
    bash_command='cd %s && dbt run-operation etl_log_update --vars \'{"loaddate":"{{ti.xcom_pull(task_ids=\'Set_Load_Date\', key=\'LoadDate\')}}", "endloaddate":"{{ti.xcom_pull(task_ids=\'Set_End_Load_Date\', key=\'EndLoadDate\')}}"}\''%dbt_path,
    dag=dag
)

Set_Load_Date >> Sync_Connection >> Latest_Loaded_TransactionDate >> Label('Imitating incremental load from full export') >> New_TransactionDate >> Init_ETL_run_Log >> Set_End_Load_Date >> Complete_ETL_run_Log

