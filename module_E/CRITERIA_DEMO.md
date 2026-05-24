# Модуль Е — демонстрация эксперту (День 3, день)

Запуск: `./scripts/run_module.sh` — выводит чек-лист.

## Е.1 Комплект документации

| Документ | Путь |
|----------|------|
| Обзор | `docs/SYSTEM_OVERVIEW.md` |
| Потоки | `docs/DATA_FLOWS.md` |
| Эксплуатация | `docs/OPERATIONS.md` |
| Критерии → артефакты | `docs/CRITERIA_MAPPING.md` |
| Диаграммы | `../module_A/diagrams/*.drawio` |
| Отчёты модулей | `../module_A/REPORT.md` … `../module_D/REPORT.md` |
| DAG | `../module_G/airflow/`, `../module_D/airflow/` |

---

## Е.2 Презентация

`presentation/OUTLINE.md` → файл `presentation/project.pptx` (создать по outline).

---

## Е.3 Сквозная демонстрация (15–20 мин)

Выполнять **по папкам**, показывая `CRITERIA_DEMO.md` каждого модуля:

| Шаг | Модуль | Что показать |
|-----|--------|----------------|
| 1 | А | draw.io + `SHOW TABLES` PG/CH + Kafka topics |
| 2 | Б | `output/demo_detection_*.jpg` + строка в `silver_detections` + MinIO |
| 3 | В | `streaming_metrics` + `gold_incidents` + лог аномалий |
| 4 | Г | `gold_traffic_aggregates` + watermark + Airflow UI |
| 5 | Д | `gold_predictions` + Metabase дашборд |
| 6 | Е | презентация + ценность для заказчика |

**Команда быстрой проверки Gold:**
```sql
SELECT 'stream' AS layer, count() FROM transport.streaming_metrics
UNION ALL SELECT 'batch', count() FROM transport.gold_traffic_aggregates
UNION ALL SELECT 'forecast', count() FROM transport.gold_predictions
UNION ALL SELECT 'incidents', count() FROM transport.gold_incidents;
```

---

## Е.4 Ценность для заказчика

- Realtime: скорость и инциденты → оперативное реагирование
- Batch 30 мин: управленческие срезы по направлениям
- Прогноз: упреждающие решения
- DQ: достоверность без «мусора» в витринах
