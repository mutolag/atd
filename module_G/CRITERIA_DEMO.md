# Модуль Г — демонстрация эксперту (День 2, день)

Запуск: `./scripts/run_module.sh [init|batch|dq|all]` из **`module_G/`**.

**Копии в папке:**
- `sql/dwh_silver_postgres.sql` — источник Silver
- `sql/gold_batch.sql` — витрины batch + watermark
- `scripts/kafka_to_silver.py` — при необходимости догнать Silver

---

## Г.1 Batch 30 минут

```bash
./scripts/run_module.sh batch
```

| Критерий | Строка кода |
|----------|-------------|
| Окно 30 мин | `src/silver_to_gold_batch.py:69` — `floor(..., 30min)` |
| Группировка камера+направление+тип | `src/silver_to_gold_batch.py:72-79` |
| Публикация Gold | `src/silver_to_gold_batch.py:101-107` |

**ClickHouse:**
```sql
SELECT camera_id, direction, window_start, vehicle_type,
       avg_speed_kmh, total_count
FROM transport.gold_traffic_aggregates
ORDER BY window_start DESC LIMIT 12;
```

---

## Г.2 Оркестрация + ELT

| Критерий | Где |
|----------|-----|
| Airflow DAG | `airflow/traffic_batch_dag.py:24-35` |
| Расписание */30 | `traffic_batch_dag.py:28` |
| ELT обоснование | `ELT_rationale.md` |

**Демо Airflow:**
```bash
cp airflow/traffic_batch_dag.py ~/airflow/dags/
airflow dags list | grep traffic
airflow dags trigger traffic_batch_pipeline
```

---

## Г.3 Опоздавшие данные

| Критерий | Строка кода |
|----------|-------------|
| Watermark | `src/silver_to_gold_batch.py:29-47` |
| Grace 15 мин | `src/silver_to_gold_batch.py:25-26`, `53-54` |

**ClickHouse:**
```sql
SELECT * FROM transport.gold_watermark;
```

---

## Г.4 Batch DQ

```bash
./scripts/run_module.sh dq
```

| Критерий | Строка кода |
|----------|-------------|
| Проверки | `src/data_quality_batch.py:30-55` |
| Лог без DELETE | `data_quality_batch.py:24-27` → `../logs/batch_quality/` |

```bash
tail -3 ../logs/batch_quality/CAM-001.log
```

---

## Г.5 Отчёт

`REPORT.md`, `airflow/DAG.md`

---

## День 3 — модуль Д

```bash
cd ../module_D
./scripts/run_module.sh all
```
