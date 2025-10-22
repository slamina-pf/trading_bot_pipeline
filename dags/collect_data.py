import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.tasks.collect_data import extract_data, transform_data, load_data

with DAG(
    'collect_data',
    description='Collect the data for model training from Binance API. Add the technical indicators and save into the database.',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'training', "machine_learning"],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )

    extract >> transform >> load