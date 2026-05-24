# Модуль Б — День 1 (день)

**Работайте только в `module_B/`.** Копии из модуля А уже здесь: `sql/`, `scripts/kafka_to_silver.py`, `scripts/init_kafka_topics.sh`.

## Запуск

```bash
cd ~/data_analitick_3_11_v3/module_B
source ../.venv/bin/activate

./scripts/run_module.sh init    # Silver + Kafka (локальные копии SQL)
./scripts/run_module.sh demo    # скрины YOLO → output/
./scripts/run_module.sh silver  # терминал 1: Kafka → PG
./scripts/run_module.sh detect  # терминал 2: YOLO → Kafka + MinIO

# или всё по порядку:
./scripts/run_module.sh all
```

Модель: `../models/yolov8n.pt` (см. `../models/README.md`).

## Демонстрация эксперту

**[CRITERIA_DEMO.md](CRITERIA_DEMO.md)** — таблицы с номерами строк в `src/stream_processor.py`, SQL, MinIO, Kafka.

## Содержимое

| Путь | Назначение |
|------|------------|
| `src/stream_processor.py` | YOLO + Kafka + MinIO |
| `src/visual_demo.py` | Визуал для эксперта |
| `src/perspective_transform.py` | Скорость с перспективой |
| `config/cameras.yaml` | Камеры без правки кода |
| `sql/dwh_silver_postgres.sql` | копия из А |
| `REPORT.md` | Отчёт + обоснование YOLO |

## День 2

```bash
cd ../module_V
./scripts/run_module.sh all
```
