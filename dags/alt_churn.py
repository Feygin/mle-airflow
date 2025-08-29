# dags/alt_churn.py
import pendulum
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Steps
from steps.churn import create_table, extract, transform, load
# Telegram
from steps.messages import (
    send_telegram_failure_message,
    send_telegram_success_message,
)

default_args = {
    # Send Telegram on any task failure
    "on_success_callback": send_telegram_success_message,
    "on_failure_callback": send_telegram_failure_message,
}

with DAG(
    dag_id="prepare_alt_churn_dataset",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    default_args=default_args,
) as dag:

    t_create = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_args=[t_extract.output],  # pull from XCom
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
        op_args=[t_transform.output],  # pull from XCom
    )


    chain(t_create, t_extract, t_transform, t_load)
