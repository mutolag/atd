#!/usr/bin/env bash
# Модуль В — День 2, первая половина. Копии из А/Б внутри module_V/.
set -euo pipefail
MODULE_V="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$MODULE_V/.." && pwd)"
cd "$ROOT"
[[ -f .venv/bin/activate ]] && source .venv/bin/activate
set -a; [[ -f .env ]] && source .env; set +a

MODE="${1:-stream}"

run_init() {
  echo "=== [В] Gold streaming + incidents ==="
  clickhouse-client --password "${CH_PASSWORD:-user}" --multiquery < "$MODULE_V/sql/gold_streaming.sql"
  mkdir -p "$ROOT/logs/anomalies"
}

run_support() {
  echo "=== [В] Поддержка потока: Silver consumer (если ещё не запущен) ==="
  python "$MODULE_V/scripts/kafka_to_silver.py" &
  sleep 2
}

run_stream() {
  echo "=== [В] Потоковая аналитика (нужны сообщения в Kafka от модуля Б) ==="
  python "$MODULE_V/src/streaming_processor.py"
}

case "$MODE" in
  init)    run_init ;;
  support) run_init; run_support; wait ;;
  stream)  run_init; run_stream ;;
  all)     run_init; run_support; run_stream ;;
  *) echo "Usage: $0 [init|support|stream|all]"; exit 1 ;;
esac
