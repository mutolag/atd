# Модуль А — демонстрация эксперту (День 1, утро)

Запуск: `./scripts/run_module.sh` из папки `module_A/`.

## А.1 Требования

| Критерий | Где показать |
|----------|----------------|
| FN/NFR | `requirements.md` — таблицы F1–F6, NF1–NF5 |

**Сказать эксперту:** требования заказчика формализованы и привязаны к компонентам.

---

## А.2 Стек и обоснование

| Критерий | Где показать |
|----------|----------------|
| DWH, Kafka, Airflow | `tech_stack.md` — таблица + раздел «Обоснование» |
| Kimball / Medallion | `tech_stack.md` § DWH |

---

## А.3 Архитектура

| Критерий | Где показать |
|----------|----------------|
| Диаграмма системы | `diagrams/01_system_architecture.drawio` |
| Потоки данных | `diagrams/02_data_flow.drawio` |
| Kafka-буфер | `sql/kafka_topics.md` |
| Нет потери данных | `scripts/kafka_to_silver.py:53-96` — rollback, Kafka retention |
| Масштабирование камер | `sql/dwh_silver_postgres.sql:62-65` — `dim_cameras` |

**Команда — Silver:**
```sql
\c silver
SELECT * FROM dim_vehicle_types;
SELECT camera_id, location FROM dim_cameras;
```

**Команда — Gold:**
```bash
clickhouse-client -q "SHOW TABLES FROM transport"
```

**Команда — Kafka:**
```bash
bash scripts/init_kafka_topics.sh
# ~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## А.4 Физический DWH

| Объект | Файл |
|--------|------|
| Справочники | `sql/dwh_silver_postgres.sql:15-25` |
| Факты | `sql/dwh_silver_postgres.sql:27-55` |
| Gold-витрины | `sql/dwh_gold_clickhouse.sql` |
| Kafka-топики | `scripts/init_kafka_topics.sh:31-34` |

---

## А.5 Отчёт

`REPORT.md`

---

## Переход к модулю Б

```bash
cd ../module_B
./scripts/run_module.sh demo
```
