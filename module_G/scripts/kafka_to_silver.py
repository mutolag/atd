#!/usr/bin/env python3
"""Kafka → PostgreSQL (Silver). Копия из module_A (проверка Silver в модуле Г)."""
import json
import os
import uuid
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_DETECTIONS", "raw_detections")
PG_DSN = (
    f"host={os.getenv('PG_HOST','localhost')} port={os.getenv('PG_PORT','5432')} "
    f"dbname={os.getenv('PG_DB','silver')} user={os.getenv('PG_USER','postgres')} "
    f"password={os.getenv('PG_PASSWORD','postgres')}"
)

INSERT_DET = """
INSERT INTO silver_detections (
    detection_id, camera_id, event_time, frame_id, track_id, vehicle_type_id,
    bbox_x1, bbox_y1, bbox_x2, bbox_y2, confidence, speed_kmh, direction,
    frame_path, track_enter, track_exit
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (detection_id) DO NOTHING
"""

INSERT_HIST = """
INSERT INTO silver_track_history (camera_id, track_id, event_time, center_x, center_y, speed_kmh, direction)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""


def parse_ts(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", ""))


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="silver-loader-v3",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    print("Silver consumer started")

    for msg in consumer:
        d = msg.value
        try:
            with conn.cursor() as cur:
                det_id = d.get("detection_id") or str(uuid.uuid4())
                cur.execute(
                    INSERT_DET,
                    (
                        det_id,
                        d["camera_id"],
                        parse_ts(d["timestamp"]),
                        d.get("frame_id"),
                        d.get("track_id"),
                        d.get("vehicle_type_id"),
                        d.get("bbox_x1"),
                        d.get("bbox_y1"),
                        d.get("bbox_x2"),
                        d.get("bbox_y2"),
                        d.get("confidence"),
                        d.get("speed_kmh"),
                        d.get("direction"),
                        d.get("frame_path"),
                        parse_ts(d.get("track_enter")),
                        parse_ts(d.get("track_exit")),
                    ),
                )
                cx = (d.get("bbox_x1", 0) + d.get("bbox_x2", 0)) / 2
                cy = (d.get("bbox_y1", 0) + d.get("bbox_y2", 0)) / 2
                cur.execute(
                    INSERT_HIST,
                    (
                        d["camera_id"],
                        d.get("track_id"),
                        parse_ts(d["timestamp"]),
                        cx,
                        cy,
                        d.get("speed_kmh"),
                        d.get("direction"),
                    ),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"DB error (message retained in Kafka): {e}")


if __name__ == "__main__":
    main()
