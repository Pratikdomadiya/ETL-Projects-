"""
write important info about this file here.
- Conditional branching with Task groups and Edge labels
- For more clear DAG representation

"""


from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago
from random import choice
import pandas as pd
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args ={
    'owner':'Pratik'
}


DATASET_PATH = '/Users/pratikdomadiya/airflow/datasets/insurance.csv'
OUTPUT_PATH = '/Users/pratikdomadiya/airflow/output/{0}.csv'

def read_csv_file():
    df = pd.read_csv(DATASET_PATH)
    print(df)
    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids = "read_csv_file") # pull the data from other task 
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)

    ti.xcom_push(key="my_clean_csv", value = df.to_json()) # save the result into ti and will be accessed by other task via key value 
    return df.to_json()

def determine_branch(ti):
    transform_action  = Variable.get("transform_action",default_var=None)
    json_data = ti.xcom_pull(key ='my_clean_csv' ) # pull result data from other task with key val pair.
    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action=='groupby_region_smoker':
        return "grouping.{0}".format(transform_action)

def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key ='my_clean_csv' ) # pull result data from other task with key val pair.
    df =  pd.read_json(json_data)
    region_df = df[df['region']=='southwest']
    region_df.to_csv(OUTPUT_PATH.format('southwest'),index=False)

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key ='my_clean_csv' ) # pull result data from other task with key val pair.
    df =  pd.read_json(json_data)
    region_df = df[df['region']=='southeast']
    region_df.to_csv(OUTPUT_PATH.format('southeast'),index=False)

def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key ='my_clean_csv' ) # pull result data from other task with key val pair.
    df =  pd.read_json(json_data)
    region_df = df[df['region']=='northwest']
    region_df.to_csv(OUTPUT_PATH.format('northwest'),index=False)

def filter_by_northeast(ti):
    json_data = ti.xcom_pull(key ='my_clean_csv' ) # pull result data from other task with key val pair.
    df =  pd.read_json(json_data)
    region_df = df[df['region']=='northeast']
    region_df.to_csv(OUTPUT_PATH.format('northeast'),index=False)

def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(key ='my_clean_csv' ) # pull result data from other task with key val pair.
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg(
        {
            'age':'mean',
            'bmi' : 'mean',
            'charges':'mean'
        }).reset_index()
    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'),index=False)

    smoker_df = df.groupby('smoker').agg(
        {
            'age':'mean',
            'bmi' : 'mean',
            'charges':'mean'
        }).reset_index()
    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'),index=False)

# initiate a DAG  
with DAG(
    dag_id="executing_branching",
    description='running branching pipelines',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['branching','conditions','python','transform','pipeline']
) as dag:
    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing: #define task group
        read_csv_file : PythonOperator(
            task_id = 'read_csv_file',
            python_callable= read_csv_file
        )
        remove_null_values : PythonOperator(
            task_id = 'remove_null_values',
            python_callable= remove_null_values
        )
        read_csv_file>>remove_null_values



    determine_branch : BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable= determine_branch
    )

    with TaskGroup('filtering') as filtering:
        filter_by_southwest : PythonOperator(
            task_id = 'filter_by_southwest',
            python_callable= filter_by_southwest
        )
        filter_by_southeast : PythonOperator(
            task_id = 'filter_by_southeast',
            python_callable= filter_by_southeast
        )
        filter_by_northwest : PythonOperator(
            task_id = 'filter_by_northwest',
            python_callable= filter_by_northwest
        )
        filter_by_northeast : PythonOperator(
            task_id = 'filter_by_northeast',
            python_callable= filter_by_northeast
        )

    with TaskGroup('grouping') as grouping:
        groupby_region_smoker = PythonOperator(
            task_id = 'groupby_region_smoker',
            python_callable= groupby_region_smoker

        )

#pipeline Order 
reading_and_preprocessing>>Label('preprocessed data')>>determine_branch>>Label('branch on condition')>>[filtering,grouping]