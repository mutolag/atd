# DAG traffic_batch_pipeline

```
silver_to_gold_batch  →  batch_data_quality
```

- **Расписание:** `*/30 * * * *`
- **Файл:** `traffic_batch_dag.py`
- **Теги:** module_G, gold

ML-прогноз — отдельный DAG в `module_D/airflow/ml_forecast_dag.py` (тоже */30).
