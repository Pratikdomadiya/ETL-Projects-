from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Pratik'
}

'''
PULL VALUES FROM OTHER TASK AND USE IT.
X-COMMUNICATION BETWEEN DIFFRENT TASKS.
'''

def increment_by_1(value): # in production you must have to define your python code in separate file.
    print("value {value}".format(value=value))
    return value+1


def multiply_by_100(ti): # ti represent as "task instances"
    value = ti.xcom_pull(task_ids = 'increment_by_1') # pull the values from other task (increment_by_1)
    print("Value {value}!".format(value=value))
    return value*100

def subtract_by_9(ti): # ti represent as "task instances"
    value = ti.xcom_pull(task_ids = 'subtract_by_9') # pull the values from other task (increment_by_1)
    print("Value {value}!".format(value=value))
    return value-9

def print_values(ti): # in production you must have to define your python code in separate file.
    value = ti.xcom_pull(task_ids = 'subtract_by_9') # pull the values from other task (increment_by_1)
    print("value {value}".format(value=value))
    

with DAG(
    dag_id="execute_python_operators_2",
    description="python operators 2 in DAGs",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['simple','python']
) as dag:
    increment_by_1 = PythonOperator(
        task_id = 'increment_by_1',
        python_callable=increment_by_1, # ref python functions
        op_kwargs = {'value':100}# pass arguments to the callable function
    )
    multiply_by_100 = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable=multiply_by_100, # ref python functions
    )
    subtract_by_9 = PythonOperator(
        task_id = 'subtract_by_9',
        python_callable=subtract_by_9, # ref python functions
    )
    print_values = PythonOperator(
        task_id = 'print_values',
        python_callable=print_values, # ref python functions
    )
    
increment_by_1 >> multiply_by_100 >> subtract_by_9 >> print_values

