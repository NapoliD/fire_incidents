# dags/fire_etl_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Asegúrate de que Airflow encuentre tu script
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "etl"))
from etl_fire import run_etl

# Parámetros por defecto
default_args = {
    "owner":           "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="fire_incident_etl",
    default_args=default_args,
    description="ETL diario de incidentes de fuego a DW Postgres",
    schedule_interval="0 2 * * *",      
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=["fire", "dw", "postgres"],
) as dag:

    etl = PythonOperator(
        task_id="etl_fire",
        python_callable=run_etl,
    )

    etl
