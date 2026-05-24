# Модуль Д — День 3 (утро)

**Только `module_D/`.** Есть копия batch-пайплайна (`src/silver_to_gold_batch_ref.py`) и SQL для Metabase/ML.

## Запуск

```bash
cd ~/data_analitick_3_11_v3/module_D
source ../.venv/bin/activate

./scripts/run_module.sh all
# = init CH + prepare batch (если пусто) + Prophet
```

Metabase: **[metabase/dashboard_setup.md](metabase/dashboard_setup.md)**

## Демонстрация эксперту

**[CRITERIA_DEMO.md](CRITERIA_DEMO.md)** — Prophet, MAE/RMSE, `gold_predictions`, дашборд.

## День 3 (день)

```bash
cd ../module_E
./scripts/run_module.sh
```
