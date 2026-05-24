#!/usr/bin/env bash
# Создание Kafka-топиков. Работает с kafka-topics.sh (типичная установка в ~/kafka).
set -euo pipefail

BOOT="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_HOME:-$HOME/kafka}/bin"

if [[ -x "${KAFKA_BIN}/kafka-topics.sh" ]]; then
  KAFKA_TOPICS="${KAFKA_BIN}/kafka-topics.sh"
elif [[ -x "${KAFKA_BIN}/kafka-topics" ]]; then
  KAFKA_TOPICS="${KAFKA_BIN}/kafka-topics"
elif command -v kafka-topics.sh &>/dev/null; then
  KAFKA_TOPICS="$(command -v kafka-topics.sh)"
elif command -v kafka-topics &>/dev/null; then
  KAFKA_TOPICS="$(command -v kafka-topics)"
else
  echo "Ошибка: не найден kafka-topics.sh в ${KAFKA_BIN}" >&2
  echo "Укажите путь: export KAFKA_HOME=\$HOME/kafka" >&2
  echo "Или запустите вручную:" >&2
  echo "  ~/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic streaming_metrics --partitions 3 --replication-factor 1" >&2
  exit 1
fi

create_topic() {
  local name=$1 parts=$2
  "$KAFKA_TOPICS" --create --if-not-exists \
    --bootstrap-server "$BOOT" \
    --topic "$name" --partitions "$parts" --replication-factor 1
}

echo "Using: $KAFKA_TOPICS"
create_topic raw_detections 3
create_topic streaming_metrics 3
create_topic anomaly_log 1

"$KAFKA_TOPICS" --bootstrap-server "$BOOT" --list
echo "Kafka topics ready."
