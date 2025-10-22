import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.tasks.feature_engineering import feature_selection
from include.tasks.train_model import xgboost_model
from include.tasks.hyperparameter_tuning import hyperparameter_tuning

with DAG(
    'train_model',
    description='This DAG makes feature engineering, hyperparameter tuning and train the model for different timeframes.',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'training', "machine_learning", "xgboost"],
) as dag:

    extract = PythonOperator(
        task_id='feature_selection',
        python_callable=feature_selection,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='hyperparameter_tuning',
        python_callable=hyperparameter_tuning,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='xgboost_model',
        python_callable=xgboost_model,
        provide_context=True,
    )

    extract >> transform >> load