# Отчёт — модуль А

## Цель

Фундамент платформы: Medallion DWH, Kafka-буфер, отказоустойчивость, масштабирование камер.

## Результаты

1. **Требования** — `requirements.md` (мониторинг, real-time, batch 30 мин, прогноз, отказоустойчивость).
2. **Стек** — MinIO / PostgreSQL / ClickHouse / Kafka / Airflow / Metabase / YOLO — `tech_stack.md` с привязкой к требованиям.
3. **DWH** — Kimball: факты `silver_detections`, `silver_track_history`; измерения `dim_cameras`, `dim_vehicle_types`; Gold-витрины в ClickHouse.
4. **Диаграммы** — `diagrams/01_system_architecture.drawio`, `02_data_flow.drawio`.
5. **Реализация** — SQL применён; топики Kafka созданы; `kafka_to_silver.py` загружает детекции в Silver.

## Отказоустойчивость

Детектор → только Kafka. При недоступности PG consumer откатывает транзакцию; сообщение остаётся в топике для повторной обработки.

## Масштабирование камер

Новая камера: запись в `dim_cameras` + блок в `module_B/config/cameras.yaml`; partition key Kafka = `camera_id`.

## Проверка эксперту

```bash
./scripts/init_all.sh
psql -d silver -c "SELECT count(*) FROM dim_vehicle_types;"
clickhouse-client -q "SHOW TABLES FROM transport"
```
