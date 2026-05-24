# Kafka-топики (контур буферизации)

| Топик | Partitions | Producer | Consumer | Назначение |
|-------|------------|----------|----------|------------|
| `raw_detections` | 3 | YOLO (модуль Б) | Silver loader (А), streaming (В) | Детекции JSON |
| `streaming_metrics` | 3 | (резерв) | — | Промежуточные метрики |
| `anomaly_log` | 1 | DQ | мониторинг | Аномалии |

## Создание

```bash
bash module_A/scripts/init_kafka_topics.sh
```

## Отказоустойчивость

При сбое PostgreSQL YOLO пишет в Kafka; `kafka_to_silver` догоняет backlog после восстановления.
