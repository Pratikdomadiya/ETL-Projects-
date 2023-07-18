'''
This file contains following:
- Traditional code snippet 
- Decorator code snippet

source : https://docs.astronomer.io/learn/airflow-decorators?tab=taskflow#how-to-use-airflow-decorators
'''


import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


def _extract_bitcoin_price():
    return requests.get(API).json()["bitcoin"]


def _process_data(ti):
    response = ti.xcom_pull(task_ids="extract_bitcoin_price")
    logging.info(response)
    processed_data = {"usd": response["usd"], "change": response["usd_24h_change"]}
    ti.xcom_push(key="processed_data", value=processed_data)


def _store_data(ti):
    data = ti.xcom_pull(task_ids="process_data", key="processed_data")
    logging.info(f"Store: {data['usd']} with change {data['change']}")


with DAG(
    "classic_dag", schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False
):
    extract_bitcoin_price = PythonOperator(
        task_id="extract_bitcoin_price", python_callable=_extract_bitcoin_price
    )

    process_data = PythonOperator(task_id="process_data", python_callable=_process_data)

    store_data = PythonOperator(task_id="store_data", python_callable=_store_data)

    extract_bitcoin_price >> process_data >> store_data




############################  DECORATOR representation of the above code #############################

import logging
from datetime import datetime
from typing import Dict

import requests
from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


@dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        return requests.get(API).json()["bitcoin"]

    @task(multiple_outputs=True)
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {"usd": response["usd"], "change": response["usd_24h_change"]}

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['usd']} with change {data['change']}")

    #Combine the traditional operators with Decorators
    email_notification = EmailOperator( 
        task_id="email_notification",
        to="noreply@astronomer.io",
        subject="dag completed",
        html_content="the dag has finished",
    )

    store_data(process_data(extract_bitcoin_price())) >> email_notification


taskflow()


"""
The decorated version of the DAG eliminates the need to explicitly instantiate the PythonOperator, has much less code and is easier to read. Notice that it also doesn't require using ti.xcom_pull and ti.xcom_push to pass data between tasks. This is all handled by the TaskFlow API when you define your task dependencies with store_data(process_data(extract_bitcoin_price())).

Here are some other things to keep in mind when using decorators:

For any decorated object in your DAG file, you must call them so that Airflow can register the task or DAG (e.g. dag = taskflow()).

When you define a task, the task_id will default to the name of the function you decorated. If you want to change that, you can simply pass a task_id to the decorator as you did in the extract task above. Similarly, other task level parameters such as retries or pools can be defined within the decorator (see the example with retries above).

You can decorate a function that is imported from another file with something like the following:
from include.my_file import my_function
@task
def taskflow_func():
    my_function()

This is recommended in cases where you have lengthy Python functions since it will make your DAG file easier to read.
"""
