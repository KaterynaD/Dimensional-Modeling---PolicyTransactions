from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow.models import Connection
from airflow import settings
import os
import yaml

HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
PostgresTransformations = Variable.get('PostgresTransformations')
dbt_path = os.path.join(HOME,'dags', PostgresTransformations)
dbt_conn_id = 'dbt_policy_trn_connection'

def create_conn():
    


    with open(os.path.join(dbt_path,'profiles.yml')) as stream:
        try:  
            profile=yaml.safe_load(stream)['postgres']['outputs']['dev'] #can have a var to switch between dev and prod the same in this project

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


            return new_conn
        
        except yaml.YAMLError as exc:
            print(exc)
            return


    
    

def get_num_batches(**kwargs):

    dbt_conn=create_conn()

    pg_hook = PostgresHook(postgres_conn_id=dbt_conn.conn_id)
    sql = "SELECT num_batches+42 FROM policy_trn.etl_stg1_scd2_num_batches WHERE table_name='stg_risk'"
    records = pg_hook.get_records(sql) 
    for record in records:    
        return record[0]  

def show_num_batches(**kwargs):
    ti = kwargs['ti']
    num_batches = ti.xcom_pull(task_ids='get_num_batches')
    print(num_batches)

def print_welcome():
    print('Welcome to Airflow!')

dag = DAG('dbt_scd2_dag', default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False)




print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

get_num_batches = PythonOperator(

    task_id='get_num_batches',
    python_callable=get_num_batches,
    provide_context=True,
    dag=dag)

show_num_batches = PythonOperator(
    task_id='show_num_batches', 
    python_callable=show_num_batches,
    provide_context=True,
    dag=dag)


print_welcome_task >> get_num_batches >> show_num_batches