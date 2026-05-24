#!/usr/bin/env bash
# Модуль А — День 1, первая половина. Запуск только из module_A/.
set -euo pipefail
MODULE_A="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$MODULE_A/.." && pwd)"
cd "$ROOT"
[[ -f .venv/bin/activate ]] && source .venv/bin/activate
set -a; [[ -f .env ]] && source .env; set +a

echo "=== [А] PostgreSQL Silver ==="
PGPASSWORD="${PG_PASSWORD:-postgres}" psql -U "${PG_USER:-postgres}" -h "${PG_HOST:-localhost}" \
  -f "$MODULE_A/sql/dwh_silver_postgres.sql" || true

echo "=== [А] ClickHouse Gold (полная схема) ==="
clickhouse-client --password "${CH_PASSWORD:-user}" --multiquery < "$MODULE_A/sql/dwh_gold_clickhouse.sql"

echo "=== [А] Kafka topics ==="
bash "$MODULE_A/scripts/init_kafka_topics.sh"

mkdir -p "$ROOT/logs/anomalies" "$ROOT/logs/batch_quality" "$ROOT/models"
echo ""
echo "Модуль А: инфраструктура готова."
echo "Демонстрация эксперту: см. $MODULE_A/CRITERIA_DEMO.md"
echo "День 1 далее: cd $ROOT/module_B && ./scripts/run_module.sh"
