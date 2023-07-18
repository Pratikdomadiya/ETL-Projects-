from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'Pratik'
}

dag = DAG(
    dag_id='hello_world',
    description='first Dag sample "Hello world DAG"',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None
)

task = BashOperator(
    task_id = 'hello_world_task',
    bash_command='echo Hello world!',
    dag=dag
)

task

# create above code like this efficient way to signify the production level code
#################################
with DAG(
    dag_id='hello_world',
    description='first Dag sample "Hello world DAG"',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None
) as dag:

    BashOperator(
        task_id = 'hello_world_task',
        bash_command='echo Hello world!',
        dag=dag
    )

task
############################