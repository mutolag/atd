#!/usr/bin/env bash
# Сквозной запуск (4 терминала или фон). См. README.md
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
source .venv/bin/activate 2>/dev/null || true

echo "1) Silver consumer (модуль А)"
python module_A/scripts/kafka_to_silver.py &
PID1=$!

sleep 2
echo "2) Streaming (модуль В)"
python module_V/src/streaming_processor.py &
PID2=$!

sleep 2
echo "3) YOLO detector (модуль Б)"
python module_B/src/stream_processor.py &
PID3=$!

echo "PIDs: silver=$PID1 stream=$PID2 yolo=$PID3"
echo "Остановка: kill $PID1 $PID2 $PID3"
wait
