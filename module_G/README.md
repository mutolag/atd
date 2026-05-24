# Модуль Г — День 2 (день)

**Только `module_G/`.** Копия Silver DDL и `kafka_to_silver.py` — для проверки источника без модуля А.

## Запуск

```bash
cd ~/data_analitick_3_11_v3/module_G
source ../.venv/bin/activate

./scripts/run_module.sh all    # init + batch + DQ
```

Airflow:

```bash
cp airflow/traffic_batch_dag.py ~/airflow/dags/
```

## Демонстрация эксперту

**[CRITERIA_DEMO.md](CRITERIA_DEMO.md)** — batch 30 мин, watermark, ELT, логи DQ.

Документ ELT: **[ELT_rationale.md](ELT_rationale.md)**

## День 3

```bash
cd ../module_D
./scripts/run_module.sh all
```
