# Логическая модель DWH и соответствие критериям

## ER-связи (кратко)

```
dim_cameras ──┬──< silver_detections >── dim_vehicle_types
              └──< silver_track_history

silver_detections ──(агрегация)──> gold_traffic_aggregates
streaming_metrics, gold_incidents, gold_predictions  (Gold, ClickHouse)
silver_detections.frame_path ──> MinIO bronze-frames
```

## Таблица соответствия (критерий А.4)

| Сущность по критерию оценки | Физическая таблица | СУБД | Файл DDL | Строки |
|----------------------------|-------------------|------|----------|--------|
| Справочник «Камеры» | `dim_cameras` | PostgreSQL | `dwh_silver_postgres.sql` | 7–15 |
| Справочник «Типы транспортных средств» | `dim_vehicle_types` | PostgreSQL | `dwh_silver_postgres.sql` | 18–28 |
| «Трекеры» (траектория) | `silver_track_history` | PostgreSQL | `dwh_silver_postgres.sql` | 52–62 |
| Факт детекции (ядро) | `silver_detections` | PostgreSQL | `dwh_silver_postgres.sql` | 31–49 |
| Факт опасной ситуации | `gold_incidents` | ClickHouse | `dwh_gold_clickhouse.sql` | 38–49 |
| Прогноз средней скорости | `gold_predictions` | ClickHouse | `dwh_gold_clickhouse.sql` | 52–65 |
| Витрина realtime (скорость, интенсивность) | `streaming_metrics` | ClickHouse | `dwh_gold_clickhouse.sql` | 5–17 |
| Витрина batch 30 мин | `gold_traffic_aggregates` | ClickHouse | `dwh_gold_clickhouse.sql` | 20–35 |
| Доп. витрина (аудит DQ) | `gold_data_quality_audit` | ClickHouse | `dwh_gold_clickhouse.sql` | 68–80 |

## Объектное хранилище

| Артефакт | Технология | Bucket / путь |
|----------|------------|---------------|
| Кадры с bbox | MinIO (Bronze) | `bronze-frames/{camera_id}/{date}/frame_*.jpg` |
| Ссылка в DWH | PostgreSQL | `silver_detections.frame_path` |

## Kafka

| Топик | Назначение | Создание |
|-------|------------|----------|
| `raw_detections` | Поток детекций JSON | `scripts/init_kafka_topics.sh` |
| `streaming_metrics` | Резерв | тот же скрипт |
| `anomaly_log` | Резерв DQ | тот же скрипт |

## Интеграция Kafka → БД

Механизм: Python consumer [`scripts/kafka_to_silver.py`](../scripts/kafka_to_silver.py) (аналог коннектора: чтение топика → INSERT в PostgreSQL).
