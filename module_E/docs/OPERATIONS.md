# Эксплуатация

## Первый запуск

```bash
cd ~/data_analitick_3_11_v3
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
ln -sf ../data_analitick_3_11/yolov8n.pt models/yolov8n.pt
./scripts/init_all.sh
```

## Порядок процессов

1. `python module_A/scripts/kafka_to_silver.py`
2. `python module_V/src/streaming_processor.py`
3. `python module_B/src/stream_processor.py`
4. Airflow: `traffic_batch_pipeline`, `ml_speed_forecast`
5. Metabase — см. `module_D/metabase/dashboard_setup.md`

Или `./scripts/run_e2e.sh` (шаги 1–3).

## Логи

| Путь | Модуль |
|------|--------|
| `logs/anomalies/{camera}.log` | В |
| `logs/batch_quality/{camera}.log` | Г |

## Airflow DAG

- `module_G/airflow/traffic_batch_dag.py`
- `module_D/airflow/ml_forecast_dag.py`

Схемы: `module_G/airflow/DAG.md`.

## Диаграммы

`module_A/diagrams/` — редактировать в draw.io.

## Демо без второй камеры

```bash
export ACTIVE_CAMERAS=CAM-001
```
