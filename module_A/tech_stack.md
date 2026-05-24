# Технологический стек и обоснование (модуль А)

| Компонент | Технология | Требование |
|-----------|------------|------------|
| Bronze | MinIO | F6, сырые кадры отдельно от PG |
| Silver | PostgreSQL | ACID, FK, справочники |
| Gold | ClickHouse | OLAP, окна 30 мин, Metabase |
| Шина | Kafka | NF1 — буфер при сбое PG |
| Детекция | YOLOv8 + ByteTrack | F1, real-time |
| Поток | Python consumer | F2, низкая задержка на площадке |
| Batch/ML | Airflow | F3, F4, расписание */30 |
| BI | Metabase | F5 |
| Схемы | draw.io | Модуль Е |

## Обоснование (связь с требованиями)

**Kafka вместо прямой записи в PG:** при сбое Silver детектор продолжает `send()` в `raw_detections`. Consumer с `auto_offset_reset=earliest` восстанавливает backlog — NF1.

**Medallion:** Bronze не нагружает PG бинарниками; Silver — единый источник событий; Gold — витрины без повторного YOLO — NF2, производительность.

**ClickHouse Gold:** колоночное хранение, быстрые `GROUP BY camera_id, direction, window_start` — F3, F5.

**Airflow:** DAG batch → DQ → ML, retry, UI для экспертов — F3, F4.

**Kimball в Silver/Gold:** факты детекций и инцидентов + измерения камер и типов ТС — согласованная аналитика по потокам наблюдения.

## DWH

- Подход слоёв: **Medallion** (Bronze / Silver / Gold).
- Модель витрин: **Kimball** (звезда: факты + справочники).
