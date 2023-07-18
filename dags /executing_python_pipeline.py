import pandas as pd
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner' : 'Pratik'
}

def read_csv_file():
    df = pd.read_csv('/Volumes/D/F/Data-Engineering-practice/ETL_Projects/datasets/insurance.csv')
    print(df.head(10))
    return df.to_json()

with DAG(
    dag_id='python_pipelines',
    description='Running python pipelines',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['pyhton','transform','pipeline']
) as dag:
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )
read_csv_file