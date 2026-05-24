#!/usr/bin/env python3
"""
Модуль В: потоковые метрики, инциденты, DQ.
Аномалии → logs/anomalies/{camera_id}.log (не в DWH).

Скорость: метрическая (гомография в модуле Б, perspective_transform.py).
Опасный маневр: угол по траектории центра bbox в пикселях — проще и
достаточно для конкурса; скорость для порога маневра уже в км/ч.
Лимит скорости: speed_limit_kmh из Kafka (из cameras.yaml / dim_cameras).
"""
import json
import math
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from clickhouse_driver import Client
from kafka import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_DIR = Path(os.getenv("LOG_DIR", PROJECT_ROOT / "logs")) / "anomalies"
LOG_DIR.mkdir(parents=True, exist_ok=True)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_DETECTIONS", "raw_detections")
WINDOW_SEC = 30
PUBLISH_EVERY = 5

CH = Client(
    host=os.getenv("CH_HOST", "localhost"),
    port=int(os.getenv("CH_PORT", "9000")),
    user=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", "user"),
    database=os.getenv("CH_DATABASE", "transport"),
)

MAX_SPEED = 150
MAX_OBJECTS_PER_FRAME = 80
SPEED_JUMP = 75
SPEED_LIMIT_DEFAULT = 60.0
MANEUVER_ANGLE_DEG = 45
MANEUVER_MIN_SPEED = 40
CONGESTION_COUNT = 25
TRUCK_CONGESTION = 8

buffers = defaultdict(lambda: deque())
track_angles = defaultdict(lambda: deque(maxlen=10))
last_publish = defaultdict(float)


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def log_anomaly(camera_id, kind, detail):
    path = LOG_DIR / f"{camera_id}.log"
    line = f"{utc_now().isoformat()}\t{kind}\t{detail}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)


def validate_detection(d):
    cam = d["camera_id"]
    speed = d.get("speed_kmh") or 0
    if speed > MAX_SPEED or speed < 0:
        log_anomaly(cam, "speed_outlier", f"track={d.get('track_id')} speed={speed}")
        return False
    return True


def validate_metrics(cam, count, avg_speed):
    if count > MAX_OBJECTS_PER_FRAME:
        log_anomaly(cam, "object_count_anomaly", f"count={count}")
        return False
    if avg_speed > MAX_SPEED:
        log_anomaly(cam, "avg_speed_anomaly", f"avg={avg_speed}")
        return False
    return True


def detect_speed_violation(d, limit):
    s = d.get("speed_kmh") or 0
    if s > limit:
        return {
            "incident_type": "speed_violation",
            "value": s,
            "details": f"limit={limit}",
        }
    return None


def detect_dangerous_maneuver(camera_id, track_id, direction, speed, positions):
    """Резкий поворот траектории в кадре (пиксели). Порог скорости — км/ч из модуля Б."""
    if speed < MANEUVER_MIN_SPEED or len(positions) < 3:
        return None
    angles = []
    for i in range(1, len(positions)):
        _, x0, y0 = positions[i - 1]
        _, x1, y1 = positions[i]
        # atan2 по смещению центра bbox — без world-координат, зато стабильно на площадке
        angles.append(math.degrees(math.atan2(y1 - y0, x1 - x0)))
    if len(angles) < 2:
        return None
    delta = abs(angles[-1] - angles[-2])
    if delta > 180:
        delta = 360 - delta
    if delta >= MANEUVER_ANGLE_DEG:
        return {
            "incident_type": "dangerous_maneuver",
            "value": delta,
            "details": f"angle_delta={delta:.1f} speed={speed}",
        }
    return None


def detect_congestion(cam, direction, vehicles):
    if len(vehicles) >= CONGESTION_COUNT:
        return {
            "incident_type": "congestion",
            "value": float(len(vehicles)),
            "details": "critical_density",
        }
    return None


def detect_truck_congestion(cam, direction, vehicles):
    trucks = [v for v in vehicles if v.get("vehicle_type_id") == 7]
    if len(trucks) >= TRUCK_CONGESTION:
        return {
            "incident_type": "truck_congestion",
            "value": float(len(trucks)),
            "details": f"trucks={len(trucks)}",
        }
    return None


def publish_metrics(camera_id, direction, window):
    speeds = [x["speed_kmh"] for x in window if x.get("speed_kmh")]
    types = [x.get("vehicle_type_id") for x in window]
    if not speeds:
        return
    avg = sum(speeds) / len(speeds)
    total = len(window)
    if not validate_metrics(camera_id, total, avg):
        return
    shares = {2: 0, 3: 0, 5: 0, 7: 0}
    for t in types:
        if t in shares:
            shares[t] += 1
    den = max(total, 1)
    row = [
        camera_id,
        direction,
        utc_now(),
        float(avg),
        float(total / (WINDOW_SEC / 60.0)),
        shares[2] / den,
        shares[7] / den,
        shares[5] / den,
        shares[3] / den,
        total,
    ]
    CH.execute(
        """INSERT INTO streaming_metrics (
            camera_id, direction, timestamp, avg_speed_kmh, intensity_per_min,
            share_car, share_truck, share_bus, share_motorcycle, total_detections_window
        ) VALUES""",
        [row],
    )


def publish_incident(d, inc):
    CH.execute(
        """INSERT INTO gold_incidents (
            incident_id, camera_id, direction, incident_type, timestamp, track_id, value, details
        ) VALUES""",
        [[
            f"{d['camera_id']}-{d.get('track_id')}-{utc_now().timestamp()}",
            d["camera_id"],
            d.get("direction", "unknown"),
            inc["incident_type"],
            utc_now(),
            int(d.get("track_id") or 0),
            float(inc["value"]),
            inc["details"],
        ]],
    )


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="streaming-v3",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    track_positions = defaultdict(lambda: deque(maxlen=10))
    print("Streaming processor started")

    for msg in consumer:
        d = msg.value
        now = time.time()
        if not validate_detection(d):
            continue
        cam = d["camera_id"]
        direction = d.get("direction", "unknown")
        key = (cam, direction)
        buffers[key].append(d)
        tid = d.get("track_id")
        cx = (d.get("bbox_x1", 0) + d.get("bbox_x2", 0)) / 2
        cy = (d.get("bbox_y1", 0) + d.get("bbox_y2", 0)) / 2
        track_positions[(cam, tid)].append((now, cx, cy))

        limit = float(d.get("speed_limit_kmh") or SPEED_LIMIT_DEFAULT)
        inc = detect_speed_violation(d, limit)
        if inc:
            publish_incident(d, inc)
        inc = detect_dangerous_maneuver(
            cam, tid, direction, d.get("speed_kmh") or 0, list(track_positions[(cam, tid)])
        )
        if inc:
            publish_incident(d, inc)

        window = list(buffers[key])
        if len(window) >= 5:
            inc = detect_congestion(cam, direction, window[-30:])
            if inc:
                publish_incident(d, inc)
            inc = detect_truck_congestion(cam, direction, window[-30:])
            if inc:
                publish_incident(d, inc)

        if now - last_publish[key] >= PUBLISH_EVERY:
            publish_metrics(cam, direction, window[-100:])
            last_publish[key] = now


if __name__ == "__main__":
    main()
