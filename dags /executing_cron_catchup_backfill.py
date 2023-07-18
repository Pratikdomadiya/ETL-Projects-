"""
write important info about this file here.
- Catch up with all previous scheduled runs
- Backfill : Executing past DAG runs for a specific date range rather than all DAG runs
- for backfill : execute command like --> airflow dags backfill -s 2022-04-21 -e 2022-04-20 cron_catchup_backfill
"""


from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago
from random import choice
from airflow.models import Variable

default_args ={
    'owner':'Pratik'
}

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='taskChoose'):
        return 'taskC'
    else:
        return 'taskD'
    
def task_c():
    print('Tsk C executed')

with DAG(
    dag_id='cron_catchup_backfill',
    description='Using crons, catchup and backfill',
    default_args=default_args,
    start_date=days_ago(30),
    schedule_interval='0 */12 * * 6,0', # schedule a cron jobs (minute hour day month year)
    catchup=True # It should be False if you backfill the DAG runs
) as dag:
        taskA = BashOperator(
             task_id = 'taskA',
             bash_command='echo TASK A has executed!'
        )

        taskChoose = PythonOperator(
             task_id = 'taskChoose',
             python_callable=choose_branch
        )

        taskBranch = BranchPythonOperator(
             task_id = 'taskBranch',
             python_callable=branch
        )

        taskC = PythonOperator(
             task_id = 'taskC',
             python_callable=task_c
        )

        taskD =BashOperator(
             task_id = 'taskD',
             bash_command='echo TASK D has executed!'
        )

        taskE = EmptyOperator(
             task_id = 'taskE'
        )


taskA>>taskChoose>>taskBranch>[taskC,taskE]
taskC>>taskD