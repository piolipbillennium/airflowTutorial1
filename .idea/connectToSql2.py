import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import configparser

config = configparser.ConfigParser()
sshHook = SSHHook(ssh_conn_id='conn_to_aws_pl')
#,sql_create
def select(postgres_hook,sql_select):
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    #create_db(postgres_hook,sql_create)
    cursor.execute(sql_select)
    city_list=[city[0] for city in cursor.fetchall()]
    return city_list

def select_from_db():
    postgres_hook = PostgresHook("conn_to_psql_id")
    #city_list = select(postgres_hook,config['psql']['select'],config['psql']['db_create'])
    #, 'CREATE TABLE IF NOT EXISTS cities(cityName text, isDescFetched boolean)
    city_list =  select(postgres_hook,'SELECT cityName FROM cities WHERE isDescFetched=false;';')

    print(city_list)





default_args = {
    'owner': 'lipinskip',
    'start_date': dt.datetime(2021, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)}

with DAG('connectToSql_3_PL',
         default_args=default_args,
         schedule_interval='0 * * * *',
         ) as dag:

    select_from_db = PythonOperator(task_id='select_from_db',
                             python_callable=select_from_db)


select_from_db