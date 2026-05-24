# Модуль Б — демонстрация эксперту (День 1, день)

Запуск: `./scripts/run_module.sh [init|demo|silver|detect|all]` — **только из `module_B/`**.

В папке есть **копии из модуля А**: `sql/`, `scripts/kafka_to_silver.py`, `scripts/init_kafka_topics.sh`.

---

## Б.1 YOLO + обоснование

| Критерий | Где |
|----------|-----|
| Алгоритм | `src/stream_processor.py:28` — Ultralytics YOLO |
| Классы ТС | `src/stream_processor.py:32` — COCO 2,3,5,7 |
| Обоснование | `REPORT.md` § «Выбор технологии» |

---

## Б.2 Визуальное подтверждение

```bash
./scripts/run_module.sh demo
```

| Критерий | Где |
|----------|-----|
| Аннотированные кадры | `output/demo_detection_*.jpg` |
| Код | `src/visual_demo.py:80-87` — `model.track`, `plot()` |

**Показать эксперту:** открыть 2–3 JPG из `module_B/output/`.

---

## Б.3 Поток + детекция

```bash
./scripts/run_module.sh silver   # терминал 1
./scripts/run_module.sh detect   # терминал 2
```

| Критерий | Строка кода |
|----------|-------------|
| Подключение к видео | `src/stream_processor.py:153-156` |
| Детекция + трекинг | `src/stream_processor.py:181-182` |
| Kafka | `src/stream_processor.py:234` |
| MinIO Bronze | `src/stream_processor.py:131-137`, вызов `190` |

**Kafka (3 сообщения):**
```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic raw_detections --max-messages 1
```

---

## Б.4 Типы, направление, трекинг, время в кадре

| Критерий | Строка кода |
|----------|-------------|
| Тип ТС | `src/stream_processor.py:192-195`, payload `224-225` |
| Направление | `src/stream_processor.py:206-208`, `108-128` |
| enter/exit | `src/stream_processor.py:201-203`, `230-231` |
| История трека | `scripts/kafka_to_silver.py:28-31` → таблица `silver_track_history` |
| Перспектива / скорость | `src/perspective_transform.py:24-33`, вызов `210-214` |

**PostgreSQL:**
```sql
SELECT camera_id, track_id, vehicle_type_id, direction, speed_kmh,
       track_enter, track_exit, frame_path
FROM silver_detections
ORDER BY event_time DESC LIMIT 5;

SELECT * FROM silver_track_history ORDER BY event_time DESC LIMIT 5;

SELECT * FROM dim_vehicle_types;
```

---

## Б.5 Справочник + кадры в MinIO

| Критерий | Где |
|----------|-----|
| Справочник типов | `sql/dwh_silver_postgres.sql:15-25` |
| Путь кадра в DWH | колонка `frame_path`, MinIO bucket `bronze-frames` |

**MinIO:** в консоли `http://localhost:9001` — bucket `bronze-frames/CAM-001/...`

---

## Б.6 Масштабирование камер

| Критерий | Где |
|----------|-----|
| Конфиг без правки кода | `config/cameras.yaml` — блок `cameras:` |
| Фильтр камер | `src/stream_processor.py:63-66` — `ACTIVE_CAMERAS` |

**Демо:** показать второй блок `CAM-002` в yaml.

---

## Б.7 Документация

`README.md`, `REPORT.md`

---

## День 2

```bash
cd ../module_V
./scripts/run_module.sh all
```
