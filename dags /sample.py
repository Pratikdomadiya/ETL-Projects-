from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Pratik'
}

def print_functions_A(): # in production you must have to define your python code in separate file.
    print("The simplest possible Python Operators A!")
def print_functions_B(): # in production you must have to define your python code in separate file.
    print("The simplest possible Python Operators B!")
def print_functions_C(): # in production you must have to define your python code in separate file.
    print("The simplest possible Python Operators C!")
def print_functions_D(): # in production you must have to define your python code in separate file.
    print("The simplest possible Python Operators D!")

with DAG(
    dag_id="execute_python_operators",
    description="python operators in DAGs",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['simple','python']
) as dag:
    taskA = PythonOperator(
        task_id = 'python_taskA',
        python_callable=print_functions_A # ref python functions
    )
    taskB = PythonOperator(
        task_id = 'python_taskB',
        python_callable=print_functions_B # ref python functions
    )
    taskC = PythonOperator(
        task_id = 'python_taskC',
        python_callable=print_functions_C # ref python functions
    )
    taskD = PythonOperator(
        task_id = 'python_taskD',
        python_callable=print_functions_D # ref python functions
    )

taskA >> [taskB,taskC]
[taskB,taskC] >> taskD

