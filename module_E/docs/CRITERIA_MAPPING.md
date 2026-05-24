# Привязка к критериям оценки

**Демонстрация эксперту:** в каждом модуле файл `CRITERIA_DEMO.md` (команды + SQL + строки кода).

| Код | Аспект | Артефакт |
|-----|--------|----------|
| **А** | Требования | `module_A/requirements.md` |
| А | Стек + обоснование | `module_A/tech_stack.md` |
| А | Диаграмма DWH/Kafka/MinIO | `module_A/diagrams/*.drawio` |
| А | Физический DWH | `module_A/sql/*.sql` |
| А | Kafka + буфер | `module_A/scripts/`, `sql/kafka_topics.md` |
| А | Отчёт | `module_A/REPORT.md` |
| **Б** | YOLO + обоснование | `module_B/REPORT.md`, `src/stream_processor.py` |
| Б | Визуал | `module_B/src/visual_demo.py` → `output/` |
| Б | Поток + DWH | `stream_processor.py` + `kafka_to_silver.py` |
| Б | README | `module_B/README.md` |
| **В** | Метрики, инциденты | `module_V/src/streaming_processor.py` |
| В | DQ логи | `logs/anomalies/` |
| В | Перспектива | `module_B/src/perspective_transform.py` → speed в Kafka |
| В | Отчёт | `module_V/REPORT.md` |
| **Г** | Batch 30 мин | `module_G/src/silver_to_gold_batch.py` |
| Г | Airflow | `module_G/airflow/traffic_batch_dag.py` |
| Г | ELT | `module_G/ELT_rationale.md` |
| Г | Late data | `gold_watermark` в batch |
| Г | Batch DQ | `module_G/src/data_quality_batch.py` |
| **Д** | ML Prophet | `module_D/src/ml_forecast.py`, `ML_rationale.md` |
| Д | Metabase | `module_D/metabase/dashboard_setup.md` |
| **Е** | Комплект docs | `module_E/docs/` |
| Е | Презентация | `module_E/presentation/OUTLINE.md` |
