# Модуль Д — демонстрация эксперту (День 3, утро)

Запуск: `./scripts/run_module.sh [init|prepare|ml|all]` из **`module_D/`**.

**Копии в папке:**
- `sql/gold_predictions.sql` — таблицы для ML и Metabase
- `src/silver_to_gold_batch_ref.py` — копия batch из модуля Г (если витрина пуста)

---

## Д.1 ML Prophet

| Критерий | Где |
|----------|-----|
| Алгоритм | `src/ml_forecast.py:13` — Prophet |
| Обоснование | `ML_rationale.md` |
| Учёт camera + direction | `ml_forecast.py:54-55`, `86-87` |
| Метрики MAE/RMSE | `ml_forecast.py:67-73`, поля `93-94` |
| Горизонт 30 мин | `ml_forecast.py:26`, `75-76` |

```bash
./scripts/run_module.sh prepare   # если нет batch-данных
./scripts/run_module.sh ml
```

**ClickHouse:**
```sql
SELECT camera_id, direction, prediction_time,
       predicted_avg_speed_kmh, mae_holdout, rmse_holdout, model_version
FROM transport.gold_predictions
ORDER BY prediction_time DESC LIMIT 5;
```

| Критерий | Где |
|----------|-----|
| Airflow */30 | `airflow/ml_forecast_dag.py:26-30` |

---

## Д.2 Metabase

| Критерий | Где |
|----------|-----|
| Настройка | `metabase/dashboard_setup.md` |
| Realtime | SQL → `streaming_metrics` |
| Batch | SQL → `gold_traffic_aggregates` |
| Прогноз | SQL → `gold_predictions` |
| Инциденты + фильтры | `gold_incidents`, фильтры camera/direction/incident_type |

**Показать эксперту:** один дашборд, 4 карточки, смена фильтра `incident_type`.

---

## Д.3 Отчёт

`REPORT.md`

---

## День 3 — модуль Е

```bash
cd ../module_E
./scripts/run_module.sh
```
