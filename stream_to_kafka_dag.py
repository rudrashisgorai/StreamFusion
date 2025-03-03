"""
DAG to stream random user data to Kafka.

This DAG triggers a Python callable that streams data to a Kafka topic.
It is scheduled to run daily at 1 AM according to the provided cron expression.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from stream_to_kafka import start_streaming

# Constants for DAG configuration
DAG_ID = "random_people_names"
START_DATE = datetime(2018, 12, 21, 12, 12)
SCHEDULE_INTERVAL = "0 1 * * *"

default_args = {
    "owner": "airflow",
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    description="Streams random user data to Kafka daily.",
) as dag:
    
    kafka_data_stream_task = PythonOperator(
        task_id="kafka_data_stream",
        python_callable=start_streaming,
    )

kafka_data_stream_task
