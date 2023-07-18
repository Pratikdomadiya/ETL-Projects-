"""
write important info about this file here.
- Conditional task dependency 

"""


from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago
from random import choice


default_args ={
    'owner':'Pratik'
}

def has_driving_licence():
    return choice([True,False])
def branch(ti): #ti is built in keyword that represent "task instance"
    if ti.xcom_pull(task_ids = 'has_driving_licence'):
        return 'eleigible_to_drive' #return id of the task 
    else:
        return "not_eleigible_to_drive" #return id of the task
    
def eleigible_to_drive():
    print("you can drive because you have the licence") 
def not_eleigible_to_drive():
    print("I'm sorry!! you can not drive because you don't have the licence")




# initiate a DAG  
with DAG(
    dag_id="executing_branching",
    description='running branching pipelines',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['branching','conditions']
) as dag:
    taskA : PythonOperator(
        task_id = 'has_driving_licence',
        python_callable= has_driving_licence
    )
    taskB : PythonOperator(
        task_id = 'branch',
        python_callable= branch
    )
    taskC : PythonOperator(
        task_id = 'eleigible_to_drive',
        python_callable= eleigible_to_drive
    )
    taskD : PythonOperator(
        task_id = 'not_eleigible_to_drive',
        python_callable= not_eleigible_to_drive
    )

#pipeline Order 
taskA >> taskB >> [taskC,taskD] # taskB has a conditional choice in between taskC and TaskD