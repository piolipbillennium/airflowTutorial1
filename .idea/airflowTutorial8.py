import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_world():
    print('world')

def write_a_file():
    with open(r"/srv/airflow/dags/pl/demofile2.txt", "w") as f:
        f.write("Now the file has more content!")
        f.close()

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('airflowTutorial20_PL',
         default_args=default_args,
         schedule_interval='0 * * * *',
         ) as dag:

    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')



    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')

    print_world = PythonOperator(task_id='print_world',
                                 python_callable=print_world)


    write_file = PythonOperator(task_id='write_a_file',
                                provide_context=True,
                                python_callable=write_a_file)

print_hello >> sleep >> print_world  >> write_file
