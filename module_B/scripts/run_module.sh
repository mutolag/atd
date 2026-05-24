#!/usr/bin/env bash
# Модуль Б — День 1. Всё в module_B/ (включая копии из А).
# Использование: ./scripts/run_module.sh [init|demo|silver|detect|all]
set -euo pipefail
MODULE_B="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$MODULE_B/.." && pwd)"
cd "$ROOT"
[[ -f .venv/bin/activate ]] && source .venv/bin/activate
set -a; [[ -f .env ]] && source .env; set +a

MODE="${1:-all}"

run_init() {
  echo "=== [Б] Silver + Kafka (копия из модуля А) ==="
  PGPASSWORD="${PG_PASSWORD:-postgres}" psql -U "${PG_USER:-postgres}" -h "${PG_HOST:-localhost}" \
    -f "$MODULE_B/sql/dwh_silver_postgres.sql" || true
  bash "$MODULE_B/scripts/init_kafka_topics.sh"
  mkdir -p "$MODULE_B/output" "$ROOT/models"
}

run_demo() {
  echo "=== [Б] Визуальная демонстрация YOLO ==="
  python "$MODULE_B/src/visual_demo.py"
  ls -la "$MODULE_B/output/"
}

run_silver() {
  echo "=== [Б] Kafka → Silver (фон). Остановка: kill %1 ==="
  python "$MODULE_B/scripts/kafka_to_silver.py" &
}

run_detect() {
  echo "=== [Б] YOLO + Kafka + MinIO ==="
  export YOLO_MODEL="${YOLO_MODEL:-$ROOT/models/yolov8n.pt}"
  python "$MODULE_B/src/stream_processor.py"
}

case "$MODE" in
  init)   run_init ;;
  demo)   run_demo ;;
  silver) run_silver; wait ;;
  detect) run_detect ;;
  all)
    run_init
    run_demo
    run_silver
    sleep 2
    run_detect
    ;;
  *) echo "Usage: $0 [init|demo|silver|detect|all]"; exit 1 ;;
esac
