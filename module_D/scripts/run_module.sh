#!/usr/bin/env bash
# Модуль Д — День 3. ML + Metabase.
set -euo pipefail
MODULE_D="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$MODULE_D/.." && pwd)"
cd "$ROOT"
[[ -f .venv/bin/activate ]] && source .venv/bin/activate
set -a; [[ -f .env ]] && source .env; set +a

MODE="${1:-ml}"

run_init() {
  echo "=== [Д] Gold tables для ML и дашборда ==="
  clickhouse-client --password "${CH_PASSWORD:-user}" --multiquery < "$MODULE_D/sql/gold_predictions.sql"
}

run_prepare() {
  echo "=== [Д] Подготовка batch-данных (копия пайплайна модуля Г) ==="
  python "$MODULE_D/src/silver_to_gold_batch_ref.py"
}

run_ml() {
  echo "=== [Д] Prophet → gold_predictions ==="
  python "$MODULE_D/src/ml_forecast.py"
}

case "$MODE" in
  init)    run_init ;;
  prepare) run_prepare ;;
  ml)      run_init; run_ml ;;
  all)     run_init; run_prepare; run_ml ;;
  *) echo "Usage: $0 [init|prepare|ml|all]"; exit 1 ;;
esac

echo "Metabase: $MODULE_D/metabase/dashboard_setup.md"
echo "Airflow: $MODULE_D/airflow/ml_forecast_dag.py"
