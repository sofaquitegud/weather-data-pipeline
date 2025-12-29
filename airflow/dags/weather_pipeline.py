import sys

sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingestion.fetch_weather import fetch_data, load_to_db
from transformation.python.run_transformation import run_transformation
from transformation.python.data_quality import check_data_quality

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    "weather_pipeline",
    default_args=default_args,
    description="Fetch, transform, and validate weather data",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_ingestion():
        df = fetch_data()
        load_to_db(df)

    ingest_task = PythonOperator(
        task_id="ingest_weather", python_callable=run_ingestion
    )

    transform_task = PythonOperator(
        task_id="transform_weather", python_callable=run_transformation
    )

    quality_check_task = PythonOperator(
        task_id="data_quality_check", python_callable=check_data_quality
    )

    ingest_task >> transform_task >> quality_check_task
