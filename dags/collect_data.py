import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.tasks.collect_data.extract_data import ExtractData
from include.tasks.collect_data.load_data import LoadData

extractor = ExtractData()
loader = LoadData()

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
        python_callable=extractor.extract,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=loader.save,
        provide_context=True,
    )

    extract >> load