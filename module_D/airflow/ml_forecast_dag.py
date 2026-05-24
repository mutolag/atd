"""Airflow DAG: ML прогноз каждые 30 мин (модуль Д)."""
from datetime import datetime, timedelta
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

MODULE_D = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_D / "src"))

from ml_forecast import run_forecast_pipeline

default_args = {
    "owner": "transport_analytics",
    "start_date": datetime(2026, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ml_speed_forecast",
    default_args=default_args,
    description="Prophet forecast 30 min horizon",
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["module_D", "ml"],
) as dag:
    PythonOperator(task_id="train_and_publish_forecast", python_callable=run_forecast_pipeline)
