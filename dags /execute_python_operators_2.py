from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Pratik'
}

def greet_hello(name): # in production you must have to define your python code in separate file.
    print("Hello, {name}".format(name=__name__))

def greet_hello_with_city(name,city): # in production you must have to define your python code in separate file.
    print("Hello, {name} from {city}".format(name=name, city=city))


with DAG(
    dag_id="execute_python_operators_2",
    description="python operators 2 in DAGs",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['simple','python']
) as dag:
    taskA = PythonOperator(
        task_id = 'python_taskA',
        python_callable=greet_hello, # ref python functions
        op_kwargs = {'name':'pratik'}# pass arguments to the callable function
    )
    taskB = PythonOperator(
        task_id = 'python_taskB',
        python_callable=greet_hello_with_city, # ref python functions
        op_kwargs={'name':'pratik','city':'Surat'} # pass arguments to the callable function
    )
    
taskA >> taskB

