import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pulling_api import fetch_top_coins, transform_pipeline,load_to_postgres

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Crypto_Info_Pipeline",
    default_args=default_args,
    description="A dag for crypto information that may be useful to someone one day",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    first_task = PythonOperator(
        task_id="extract_coins",
        python_callable=fetch_top_coins,
    )

    second_task = PythonOperator(
        task_id="data_transformation",
        python_callable=transform_pipeline,
    )
    # this last task load the data into PostreSQL warehouse
    third_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )
    first_task >> second_task >> third_task 