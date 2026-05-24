"""Airflow DAG: batch 30 min + quality (модуль Г)."""
from datetime import datetime, timedelta
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

MODULE_G = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_G / "src"))

from silver_to_gold_batch import run_batch
from data_quality_batch import run_checks

default_args = {
    "owner": "transport_analytics",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="traffic_batch_pipeline",
    default_args=default_args,
    description="30-min ELT Silver→Gold + batch DQ",
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["module_G", "gold"],
) as dag:
    batch = PythonOperator(task_id="silver_to_gold_batch", python_callable=run_batch)
    quality = PythonOperator(task_id="batch_data_quality", python_callable=run_checks)
    batch >> quality
