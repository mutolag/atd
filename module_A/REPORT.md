# Отчёт по модулю А  
## Проектирование и разработка архитектуры инфраструктуры данных

**Компетенция:** Аналитик транспортных данных  
**Проект:** Платформа мониторинга опасных транспортных ситуаций на основе городского видеонаблюдения  
**Репозиторий:** `data_analitick_3_11_v3/module_A/`  
**Отчёт со скриншотами (для экспертов):** [`REPORT.ipynb`](REPORT.ipynb)

---

## 1. Цель и контекст

Департамент транспорта города поручил построить единый аналитический контур: видеопотоки с камер → объективные метрики транспортного потока, выявление опасных ситуаций, пакетная аналитика и прогноз средней скорости для оперативного реагирования и управленческих решений.

В модуле А спроектирована и **физически реализована** инфраструктура данных: Medallion DWH (MinIO / PostgreSQL / ClickHouse), шина Apache Kafka, схемы БД, топики, consumer Kafka→Silver, архитектурная диаграмма и обоснование технологий.

---

## 2. Функциональные требования

| ID | Требование | Компонент реализации |
|----|------------|----------------------|
| F1 | Детекция ТС из видеопотока: тип, направление, трекинг, время в кадре | Модуль Б (YOLOv8); Silver: `silver_detections`, `silver_track_history` |
| F2 | Потоковые метрики и опасные ситуации в реальном времени | Модуль В → Gold: `streaming_metrics`, `gold_incidents` |
| F3 | Batch-агрегаты каждые 30 мин по камере и направлению | Модуль Г → `gold_traffic_aggregates`; Airflow DAG `traffic_batch_pipeline` |
| F4 | Прогноз средней скорости на 30 мин вперёд | Модуль Д → `gold_predictions`; Airflow DAG `ml_speed_forecast` |
| F5 | Интерактивный дашборд: realtime / batch / forecast / инциденты | Metabase (модуль Д), источник — ClickHouse Gold |
| F6 | Сохранение непустых кадров с объектами в объектное хранилище, путь в DWH | MinIO `bronze-frames`; поле `silver_detections.frame_path` (модуль Б) |

*Детализация:* приложение [`requirements.md`](requirements.md).

---

## 3. Нефункциональные требования

| ID | Требование | Реализация | Проверка |
|----|------------|------------|----------|
| NF1 | Нет потери данных при кратковременной недоступности БД | Kafka `raw_detections` + consumer с rollback | `kafka_to_silver.py:53-96` |
| NF2 | Подключение новых камер без смены архитектуры | `dim_cameras` + `cameras.yaml` (модуль Б) | `dwh_silver_postgres.sql:64-68` |
| NF3 | Аномалии не в витринах, а в логах по камерам | Модули В, Г → `logs/anomalies/`, `logs/batch_quality/` | Модуль В/Г |
| NF4 | Учёт опоздавших данных в batch | `gold_watermark` + grace 15 мин | `dwh_gold_clickhouse.sql:83-90`; модуль Г |
| NF5 | Масштабирование потока | Kafka partitions; ключ `camera_id` | `init_kafka_topics.sh:31-34` |

---

## 4. Архитектура и технологии

| Роль | Конкретная технология | Назначение |
|------|----------------------|------------|
| Объектное хранилище (Bronze) | **MinIO** | Кадры с bounding box |
| Операционный слой (Silver) | **PostgreSQL** | Факты, справочники, FK |
| Аналитическое хранилище (Gold) | **ClickHouse** | Витрины OLAP, time-series |
| Потоковая передача | **Apache Kafka** | Буфер `raw_detections` |
| Потоковая обработка | **Python** (`kafka-python`, consumers) | Silver loader, streaming |
| Детекция | **YOLOv8** + **ByteTrack** | Модуль Б |
| Оркестрация | **Apache Airflow** | Batch ELT, ML, DQ — модули Г, Д |
| Визуализация | **Metabase** | Модуль Д |
| Диаграммы | **draw.io** | `diagrams/00_platform_architecture.drawio` |

Формулировки вида «СУБД» или «хранилище» без имени продукта **не используются** — везде указаны конкретные инструменты.

---

## 5. Обоснование стека

Полное обоснование с привязкой к требованиям F/NF, надёжности и масштабированию — в [`tech_stack.md`](tech_stack.md), раздел «Обоснование стека (критерий А.2 «с»)».

**Резюме:** Kafka закрывает NF1; Medallion разделяет медиа и факты; ClickHouse — OLAP для F3/F5; Airflow — расписание 30 мин для F3/F4; YOLO — real-time F1 на ресурсах площадки.

---

## 6. Подход к проектированию DWH

Сравнение **Inmon / Kimball / Medallion**, выбранная комбинация **Medallion + Kimball** и **не менее трёх аргументов** под задачу заказчика — в [`tech_stack.md`](tech_stack.md), раздел «Подход к проектированию DWH (критерий А.2 «д»)».

**Кратко:**

1. **Medallion** — Bronze (MinIO) / Silver (PostgreSQL) / Gold (ClickHouse) по зрелости данных.  
2. **Kimball** — факты (`silver_detections`, `gold_incidents`, …) и измерения (`dim_cameras`, `dim_vehicle_types`).  
3. **Аргумент 1:** видеокадры — в объектном хранилище, не в реляционной БД.  
4. **Аргумент 2:** Kafka + consumer при сбое PG — без потери потока.  
5. **Аргумент 3:** ClickHouse Gold — быстрые витрины для дашборда и batch 30 мин.

**Оркестрация процессов (Airflow):**

| Процесс | DAG / компонент |
|---------|-----------------|
| Batch ELT Silver→Gold | `traffic_batch_pipeline` → `module_G/airflow/traffic_batch_dag.py` |
| Batch DQ | таск `batch_data_quality` в том же DAG |
| ML-прогноз | `ml_speed_forecast` → `module_D/airflow/ml_forecast_dag.py` |
| Поток realtime | `module_V/src/streaming_processor.py` (continuous) |
| Kafka→Silver | `module_A/scripts/kafka_to_silver.py` (continuous) |

---

## 7. Архитектурная диаграмма

**Единая схема** (критерий А.3): [`diagrams/00_platform_architecture.drawio`](diagrams/00_platform_architecture.drawio) — слои, ортогональные стрелки с подписями на белом фоне (не на блоках).

На диаграмме отражены:

- **Слои:** источники → обработка → шина → хранение (Bronze/Silver/Gold) → потребители  
- **Направление потоков** (подписи на стрелках): видео, JSON детекций, кадры JPG, факты SQL, метрики/инциденты, batch/прогноз  
- **Аналитическое хранилище:** ClickHouse (Gold) + PostgreSQL (Silver/ODS)  
- **Брокер:** Apache Kafka  
- **Обработчики:** YOLOv8, Streaming Processor, Airflow, Silver Loader  
- **Объектное хранилище:** MinIO  
- **Точки интеграции:** RTSP/файл (вход), Metabase и оператор (выход)

Экспорт для отчёта/презентации: File → Export → PNG → `diagrams/00_platform_architecture.png`.

*Архивные схемы `01_*`, `02_*` не используются для сдачи модуля А.*

---

## 8. Логическая модель и физическая реализация DWH

### 8.1. Соответствие сущностей критериям

| Сущность по критерию | Таблица | DDL | Строки |
|----------------------|---------|-----|--------|
| Справочник «Камеры» | `dim_cameras` | `sql/dwh_silver_postgres.sql` | 7–15 |
| Справочник «Типы ТС» | `dim_vehicle_types` | `sql/dwh_silver_postgres.sql` | 18–28 |
| «Трекеры» | `silver_track_history` | `sql/dwh_silver_postgres.sql` | 52–62 |
| Факт детекции | `silver_detections` | `sql/dwh_silver_postgres.sql` | 31–49 |
| Факт опасной ситуации | `gold_incidents` | `sql/dwh_gold_clickhouse.sql` | 38–49 |
| Прогноз средней скорости | `gold_predictions` | `sql/dwh_gold_clickhouse.sql` | 52–65 |
| Витрина realtime | `streaming_metrics` | `sql/dwh_gold_clickhouse.sql` | 5–17 |
| Витрина batch 30 мин | `gold_traffic_aggregates` | `sql/dwh_gold_clickhouse.sql` | 20–35 |
| Витрина аудита DQ | `gold_data_quality_audit` | `sql/dwh_gold_clickhouse.sql` | 68–80 |

Полная ER-описание: [`sql/schema_mapping.md`](sql/schema_mapping.md).

### 8.2. Расширение схемы (новые сущности и витрины)

Помимо базовых фактов и справочников созданы **аналитические витрины Gold** (≥2): `streaming_metrics`, `gold_traffic_aggregates`, а также `gold_incidents`, `gold_predictions`, `gold_watermark`, `gold_data_quality_audit`.

### 8.3. Объектное хранилище

- **Технология:** MinIO (S3 API)  
- **Bucket:** `bronze-frames` (создаётся при первой записи в модуле Б)  
- **Связь с DWH:** колонка `silver_detections.frame_path` (путь `s3://bronze-frames/...`)

### 8.4. Kafka-топики

Создание: [`scripts/init_kafka_topics.sh`](scripts/init_kafka_topics.sh), строки **31–34**:

- `raw_detections` (3 partitions)  
- `streaming_metrics`  
- `anomaly_log`  

Описание: [`sql/kafka_topics.md`](sql/kafka_topics.md).

### 8.5. Механизм передачи Kafka → целевая БД

**Реализация:** Python consumer [`scripts/kafka_to_silver.py`](scripts/kafka_to_silver.py)

| Фрагмент | Строки | Назначение |
|----------|--------|------------|
| SQL INSERT детекции | 19–26 | Загрузка в `silver_detections` |
| SQL INSERT трека | 28–31 | Загрузка в `silver_track_history` |
| Цикл consumer | 53–96 | Чтение топика, commit/rollback при ошибке PG |

При ошибке БД выполняется `rollback` — сообщение остаётся в Kafka для повторной обработки (NF1).

### 8.6. Инициализация инфраструктуры

Скрипт: [`scripts/run_module.sh`](scripts/run_module.sh) — PostgreSQL Silver, ClickHouse Gold, Kafka topics.

---

## 9. Проверка на рабочем месте (РМ)

```bash
cd ~/data_analitick_3_11_v3/module_A
./scripts/run_module.sh
```

**PostgreSQL (Silver):**
```sql
\c silver
SELECT * FROM dim_vehicle_types;
SELECT camera_id, location FROM dim_cameras;
\dt
```

**ClickHouse (Gold):**
```bash
clickhouse-client --password user -q "SHOW TABLES FROM transport"
clickhouse-client --password user -q "SELECT name FROM system.tables WHERE database='transport'"
```

**Kafka:**
```bash
bash scripts/init_kafka_topics.sh
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Consumer (опционально):**
```bash
python scripts/kafka_to_silver.py
```

---

## 10. Выводы

В модуле А выполнено:

- формализация **функциональных и нефункциональных** требований (разделы 2–3 настоящего отчёта);  
- выбор и **обоснование** стека с именами технологий и оркестрацией процессов;  
- **аргументированный подход** Medallion + Kimball к проектированию DWH;  
- **единая архитектурная диаграмма** с потоками и обязательными типами компонентов;  
- **физический DWH:** DDL Silver/Gold, Kafka-топики, consumer Kafka→PostgreSQL, схема MinIO.

Модули Б–Д наполняют и используют эту инфраструктуру: детекция (Б), потоковая аналитика (В), batch и Airflow (Г), ML и Metabase (Д).

---

*Приложения:* [`requirements.md`](requirements.md), [`tech_stack.md`](tech_stack.md), [`CRITERIA_DEMO.md`](CRITERIA_DEMO.md).
