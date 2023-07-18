from datetime import datetime,timedate
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'Pratik'
}

with DAG(
    dag_id='executing_multiple_tasks',
    description="DAG with multiple tasks and dependencies",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once'
    template_searchpath='path/dags/bash_script'
 ) as dag:
        taskA = BashOperator(
            task_id = 'taskA',
            bash_command='echo TASK A has eecuted!',
        )
        taskB = BashOperator(
            task_id = 'taskB',
            bash_command='echo TASK B has eecuted!',
        )
        taskC = BashOperator(
            task_id = 'taskC',
            bash_command='echo TASK C has eecuted!',
        )
        taskD = BashOperator(
            task_id = 'taskD',
            bash_command='echo TASK D has eecuted!',
        )
        taskE = BashOperator(
            task_id = 'taskE',
            bash_command='taskE.sh',
        )
        taskF = BashOperator(
            task_id = 'taskF',
            bash_command='taskF.sh',
        )
        taskG = BashOperator(
            task_id = 'taskG',
            bash_command='task.sh',
        )
        taskE = BashOperator(
            task_id = 'taskA',
            bash_command='taskE.sh',
        )


taskA.set_downstream(taskB) #set dependency of the task (A ---> B)
taskA.set_upstream(taskB) # dependency order : A <----- B

taskC.set_upstream(taskA)
taskD.set_upstream(taskA)