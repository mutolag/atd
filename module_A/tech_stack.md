# Технологический стек и обоснование (модуль А)

## Выбранный стек

| Компонент | Технология | Требование | Надёжность / масштабирование |
|-----------|------------|------------|------------------------------|
| Bronze | **MinIO** (S3 API) | F6 | Кадры отдельно от PG; горизонтальное масштабирование объектов |
| Silver | **PostgreSQL** | F1, NF2 | ACID, FK на справочники; единый источник событий |
| Gold | **ClickHouse** | F3, F5 | OLAP, окна 30 мин; партиции по времени, быстрые агрегаты |
| Шина | **Apache Kafka** | NF1 | Retention 24 ч; replay при восстановлении PG |
| Детекция | **YOLOv8** + **ByteTrack** (Ultralytics) | F1 | Real-time на CPU/GPU площадки |
| Потоковая обработка | **Python** consumer (`kafka-python`) | F2 | Низкая задержка без кластера Spark/Flink |
| Оркестрация | **Apache Airflow** | F3, F4 | DAG, расписание `*/30`, retry, UI для экспертов |
| BI | **Metabase** | F5 | Self-service, фильтры по camera/direction |
| Схемы | **draw.io** | Модуль Е | Единая архитектурная диаграмма |

## Обоснование стека (критерий А.2 «с»)

Каждая технология привязана к требованию и эксплуатационным свойствам, а не к перечню «по умолчанию».

| Технология | Требование | Почему выбрана |
|------------|------------|----------------|
| **Kafka** | NF1 | При сбое PostgreSQL детектор пишет в `raw_detections`; consumer догоняет backlog (`kafka_to_silver.py`, `auto_offset_reset=earliest`) — данные не теряются |
| **MinIO** | F6 | Бинарные кадры с bbox не в реляционной БД; снижение нагрузки на Silver и стоимость хранения |
| **PostgreSQL Silver** | F1, NF2 | Нормализованные факты, справочники камер/типов ТС, FK; удобный replay batch из одного источника |
| **ClickHouse Gold** | F3, F5 | Колоночные витрины, `GROUP BY` по `camera_id`, `direction`, `window_start` для Metabase |
| **YOLOv8** | F1 | Единый inference + трекинг; 4 класса ТС из COCO на площадке за 3 ч модуля Б |
| **Python streaming** | F2 | Потребление Kafka без отдельного кластера; задержка публикации метрик ~5 с |
| **Airflow** | F3, F4 | Явные зависимости batch → DQ → ML; расписание 30 мин для экспертов |
| **Metabase** | F5 | Интерактивный дашборд без разработки фронтенда |

## Подход к проектированию DWH (критерий А.2 «д»)

### Сравнение альтернатив

| Подход | Суть | Оценка для задачи заказчика |
|--------|------|----------------------------|
| **Inmon** | Нормализованное корпоративное хранилище, интеграция «сверху вниз» | Избыточен: тяжёлые видеокадры и потоковые события требуют отдельного контура сырых данных |
| **Kimball** | Звёздная схема: факты + измерения (`dim_cameras`, `dim_vehicle_types`) | Подходит для отчётности по камерам, направлениям, типам ТС и инцидентам |
| **Medallion** | Слои Bronze → Silver → Gold с ужесточением качества по слоям | Подходит для разделения кадров (Bronze), событий (Silver) и витрин (Gold) |

### Выбранное решение

**Medallion (слои хранения) + Kimball (логическая модель фактов и справочников в Silver/Gold).**

### Аргументы в пользу выбора (≥2)

1. **Неструктурированные данные видеонаблюдения.** Кадры с ограничивающими рамками — бинарные объекты большого объёма. Размещение в **Bronze (MinIO, bucket `bronze-frames`)** отделяет медиа от реляционных фактов в PostgreSQL и соответствует предметной области «видео → события → аналитика».

2. **Отказоустойчивость при недоступности БД.** Заказчик требует отсутствия потери данных. **Apache Kafka** буферизует поток детекций; наполнение Silver выполняет consumer [`scripts/kafka_to_silver.py:53-96`](scripts/kafka_to_silver.py) с откатом транзакции при ошибке PG и повторным чтением из топика.

3. **OLAP и прогноз на витринах.** Пакетные агрегаты за 30 мин и прогноз скорости требуют быстрых сканов по времени и измерениям. **ClickHouse (Gold)** с витринами `streaming_metrics`, `gold_traffic_aggregates`, `gold_incidents`, `gold_predictions` оптимизирован под дашборд Metabase (F3, F4, F5).

### Оркестрация процессов (критерий А.2 — Airflow)

| Процесс | Технология | Где реализовано | Расписание |
|---------|------------|-----------------|------------|
| ELT Silver → Gold (batch 30 мин) | **Apache Airflow** | DAG `traffic_batch_pipeline` — [`module_G/airflow/traffic_batch_dag.py`](../module_G/airflow/traffic_batch_dag.py) | `*/30 * * * *` |
| Поиск аномалий в batch-витринах | **Apache Airflow** | Таск `batch_data_quality` в том же DAG | после batch |
| ML-прогноз средней скорости | **Apache Airflow** | DAG `ml_speed_forecast` — [`module_D/airflow/ml_forecast_dag.py`](../module_D/airflow/ml_forecast_dag.py) | `*/30 * * * *` |
| Потоковые метрики и инциденты | **Python** (continuous consumer) | [`module_V/src/streaming_processor.py`](../module_V/src/streaming_processor.py) | по мере поступления Kafka |
| Детекция и публикация в Kafka | **YOLOv8** (модуль Б) | [`module_B/src/stream_processor.py`](../module_B/src/stream_processor.py) | непрерывно по камерам |
| Kafka → PostgreSQL (Silver) | **Python** consumer | [`module_A/scripts/kafka_to_silver.py`](scripts/kafka_to_silver.py) | непрерывно |

Пакетная оркестрация вынесена в Airflow; потоковая — в долгоживущие consumers, чтобы не смешивать latency real-time и batch-окна.

## Физическая реализация DWH

- DDL Silver: [`sql/dwh_silver_postgres.sql`](sql/dwh_silver_postgres.sql)
- DDL Gold: [`sql/dwh_gold_clickhouse.sql`](sql/dwh_gold_clickhouse.sql)
- Соответствие сущностей критериям: [`sql/schema_mapping.md`](sql/schema_mapping.md)
- Архитектурная диаграмма: [`diagrams/00_platform_architecture.drawio`](diagrams/00_platform_architecture.drawio)
