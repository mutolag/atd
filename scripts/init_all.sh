#!/usr/bin/env bash
# Инициализация DWH (модуль А) и каталогов логов
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== PostgreSQL Silver ==="
PGPASSWORD="${PG_PASSWORD:-postgres}" psql -U "${PG_USER:-postgres}" -h "${PG_HOST:-localhost}" -f module_A/sql/dwh_silver_postgres.sql || true

echo "=== ClickHouse Gold ==="
clickhouse-client --password "${CH_PASSWORD:-user}" --multiquery < module_A/sql/dwh_gold_clickhouse.sql

echo "=== Kafka topics ==="
bash module_A/scripts/init_kafka_topics.sh

mkdir -p logs/anomalies logs/batch_quality models module_B/output
echo "Готово. Модель: ln -sf ../data_analitick_3_11/yolov8n.pt models/yolov8n.pt"
