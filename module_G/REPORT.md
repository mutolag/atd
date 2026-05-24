# Отчёт — модуль Г

## Batch 30 минут

` silver_to_gold_batch.py`:
- Чтение `silver_detections` с `is_anomaly = false`
- Окна `floor(event_time, 30 min)`
- Группировка: `camera_id`, `direction`, `vehicle_type`, метрики count/avg/p25/p75/share
- Запись в `gold_traffic_aggregates`

## Опоздавшие данные

Таблица `gold_watermark`, grace 15 мин — пересчёт затрагивает late rows без потери.

## Оркестрация

DAG `traffic_batch_pipeline`: `silver_to_gold_batch` → `batch_data_quality`, schedule `*/30`.

## Качество batch

`data_quality_batch.py` — скорость >140, счётчики, всплеск инцидентов → лог, без DELETE.

## ELT

Документ `ELT_rationale.md`.
