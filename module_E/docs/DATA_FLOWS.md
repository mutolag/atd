# Потоки данных

## 1. Детекция (модуль Б)

```
Video/RTSP → YOLO+ByteTrack → Kafka raw_detections
                          ↘ MinIO bronze-frames (кадры)
```

## 2. Silver (модуль А)

```
Kafka raw_detections → kafka_to_silver → PostgreSQL
  silver_detections, silver_track_history
  dim_cameras, dim_vehicle_types
```

## 3. Потоковая Gold (модуль В)

```
Kafka raw_detections → streaming_processor → ClickHouse
  streaming_metrics, gold_incidents
Аномалии → logs/anomalies/{camera}.log (не в CH)
```

## 4. Batch Gold (модуль Г)

```
PostgreSQL silver_detections → silver_to_gold_batch → gold_traffic_aggregates
Airflow */30 → data_quality_batch → logs/batch_quality/
```

## 5. ML (модуль Д)

```
gold_traffic_aggregates → Prophet → gold_predictions
Metabase ← ClickHouse (все витрины)
```

## Kafka-топики

| Топик | Назначение |
|-------|------------|
| raw_detections | Детекции от YOLO |
| streaming_metrics | (резерв, основной путь — CH напрямую) |
| anomaly_log | (резерв) |

См. `module_A/sql/kafka_topics.md`.
