#!/usr/bin/env bash
# Модуль Г — День 2. Batch + DQ + Airflow.
set -euo pipefail
MODULE_G="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$MODULE_G/.." && pwd)"
cd "$ROOT"
[[ -f .venv/bin/activate ]] && source .venv/bin/activate
set -a; [[ -f .env ]] && source .env; set +a

MODE="${1:-batch}"

run_init() {
  echo "=== [Г] Gold batch tables ==="
  clickhouse-client --password "${CH_PASSWORD:-user}" --multiquery < "$MODULE_G/sql/gold_batch.sql"
  mkdir -p "$ROOT/logs/batch_quality"
}

run_batch() {
  echo "=== [Г] ELT Silver → Gold (30 мин) ==="
  python "$MODULE_G/src/silver_to_gold_batch.py"
}

run_dq() {
  echo "=== [Г] Batch quality checks ==="
  python "$MODULE_G/src/data_quality_batch.py"
}

case "$MODE" in
  init)  run_init ;;
  batch) run_init; run_batch ;;
  dq)    run_dq ;;
  all)   run_init; run_batch; run_dq ;;
  *) echo "Usage: $0 [init|batch|dq|all]"; exit 1 ;;
esac

echo "Airflow DAG: $MODULE_G/airflow/traffic_batch_dag.py → ~/airflow/dags/"
