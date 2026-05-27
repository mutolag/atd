#!/usr/bin/env python3
"""
Модуль Б: YOLOv8 + ByteTrack, Kafka, MinIO (Bronze).
Масштабирование: config/cameras.yaml

Важно: у каждой камеры своя копия модели YOLO (PyTorch не thread-safe).
"""
import json
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
import sys

MODULE_B = Path(__file__).resolve().parents[1]
PROJECT_ROOT = MODULE_B.parent
sys.path.insert(0, str(MODULE_B / "src"))

import boto3
import cv2
import numpy as np
import yaml
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from ultralytics import YOLO

from perspective_transform import PerspectiveTransformer

VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

# Kafka/MinIO — общие; producer thread-safe для send()
_producer = None
_s3 = None
_s3_lock = threading.Lock()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def utc_date_str():
    return datetime.now(timezone.utc).strftime("%Y%m%d")


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def load_config():
    cfg_path = MODULE_B / "config" / "cameras.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        cameras = yaml.safe_load(f)["cameras"]
    only = os.getenv("ACTIVE_CAMERAS", "").strip()
    if only:
        ids = {x.strip() for x in only.split(",")}
        cameras = [c for c in cameras if c["camera_id"] in ids]
    return cameras


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_DETECTIONS", "raw_detections")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9001")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze-frames")
MODEL_PATH = os.getenv("YOLO_MODEL", str(PROJECT_ROOT / "models" / "yolov8n.pt"))


def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, cls=NumpyEncoder).encode("utf-8"),
            max_block_ms=10000,
        )
    return _producer


def get_s3():
    global _s3
    if _s3 is None:
        with _s3_lock:
            if _s3 is None:
                client = boto3.client(
                    "s3",
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                )
                try:
                    client.head_bucket(Bucket=MINIO_BUCKET)
                except ClientError as e:
                    if e.response["Error"]["Code"] == "404":
                        client.create_bucket(Bucket=MINIO_BUCKET)
                _s3 = client
    return _s3


def determine_direction(positions, direction_cfg=None):
    """Направление in/out по векторам из cameras.yaml (конкурс: потоки наблюдения)."""
    if len(positions) < 2:
        return "unknown"
    _, x0, y0 = positions[-2]
    _, x1, y1 = positions[-1]
    dx, dy = x1 - x0, y1 - y0
    mag = (dx * dx + dy * dy) ** 0.5
    if mag < 3.0:
        return "unknown"
    if not direction_cfg:
        direction_cfg = {"in": [0, 1], "out": [1, 0]}
    in_v = direction_cfg.get("in", [0, 1])
    out_v = direction_cfg.get("out", [1, 0])
    dot_in = (dx * in_v[0] + dy * in_v[1]) / mag
    dot_out = (dx * out_v[0] + dy * out_v[1]) / mag
    if dot_in > 0.35 and dot_in >= dot_out:
        return "in"
    if dot_out > 0.35 and dot_out > dot_in:
        return "out"
    return "unknown"


def save_frame_to_minio(frame, camera_id, frame_id):
    key = f"{camera_id}/{utc_date_str()}/frame_{frame_id}.jpg"
    _, buf = cv2.imencode(".jpg", frame)
    get_s3().put_object(
        Bucket=MINIO_BUCKET, Key=key, Body=buf.tobytes(), ContentType="image/jpeg"
    )
    return f"s3://{MINIO_BUCKET}/{key}"


def process_camera(camera_cfg):
    camera_id = camera_cfg["camera_id"]
    source = camera_cfg["video_source"]
    if not os.path.isabs(source):
        source = str((PROJECT_ROOT / source).resolve())

    if not Path(source).exists():
        print(f"[{camera_id}] ERROR: video not found: {source}")
        return

    print(f"[{camera_id}] Loading YOLO model...")
    model = YOLO(MODEL_PATH)

    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        print(f"[{camera_id}] ERROR: cannot open {source}")
        return

    print(f"[{camera_id}] Started: {source}")

    transformer = PerspectiveTransformer(
        camera_cfg["perspective_points"],
        camera_cfg["real_world_rectangle"]["width_m"],
        camera_cfg["real_world_rectangle"]["height_m"],
    )
    frame_interval = camera_cfg.get("frame_save_interval", 15)
    frame_id = 0
    tracks_state = defaultdict(lambda: {
        "enter_time": None,
        "exit_time": None,
        "positions": deque(maxlen=30),
        "direction": "unknown",
    })
    producer = get_producer()

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        frame_id += 1
        ts = utc_now_iso()
        results = model.track(
            frame, persist=True, classes=list(VEHICLE_CLASSES.keys()), verbose=False
        )
        #отрисовка обработанного видео потока
        # frame1 = results[0].plot()
        # cv2.imshow('trump', frame1)
        # if cv2.waitKey(1) & 0xFF == ord('q'):
        #     break
        # frame_path = None
        has_objects = False

        if results and results[0].boxes is not None and len(results[0].boxes):
            has_objects = True
            frame_path = save_frame_to_minio(frame, camera_id, frame_id)
        
        for box in results[0].boxes:
            cls_id = int(box.cls[0])
            if cls_id not in VEHICLE_CLASSES:
                continue
            track_id = int(box.id[0]) if box.id is not None else -1
            x1, y1, x2, y2 = map(float, box.xyxy[0])
            cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
            conf = float(box.conf[0])
            st = tracks_state[track_id]
            now_ts = time.time()
            if st["enter_time"] is None:
                st["enter_time"] = ts
            st["exit_time"] = ts
            st["positions"].append((now_ts, cx, cy))
            direction = determine_direction(
                st["positions"], camera_cfg.get("directions")
            )
            st["direction"] = direction

            speed_kmh = 0.0
            if len(st["positions"]) >= 2:
                t0, x0, y0 = st["positions"][-2]
                t1, x1p, y1p = st["positions"][-1]
                speed_kmh = transformer.speed_kmh(x0, y0, t0, x1p, y1p, t1)

            payload = {
                "detection_id": str(uuid.uuid4()),
                "camera_id": camera_id,
                "speed_limit_kmh": float(camera_cfg.get("speed_limit_kmh", 60)),
                "timestamp": ts,
                "frame_id": frame_id,
                "track_id": track_id,
                "vehicle_type_id": cls_id,
                "vehicle_type": VEHICLE_CLASSES[cls_id],
                "bbox_x1": x1, "bbox_y1": y1, "bbox_x2": x2, "bbox_y2": y2,
                "confidence": conf,
                "speed_kmh": round(speed_kmh, 2),
                "direction": direction,
                "frame_path": frame_path,
                "track_enter": st["enter_time"],
                "track_exit": st["exit_time"],
            }
            producer.send(KAFKA_TOPIC, key=camera_id.encode(), value=payload)

        if frame_id % 100 == 0:
            print(f"[{camera_id}] frame {frame_id}, detections sent")

    cap.release()
    print(f"[{camera_id}] finished ({frame_id} frames)")


def main():
    if not Path(MODEL_PATH).exists():
        print(f"ERROR: model not found: {MODEL_PATH}")
        sys.exit(1)

    cameras = load_config()
    if not cameras:
        print("ERROR: no cameras in config (check ACTIVE_CAMERAS)")
        sys.exit(1)

    print(f"Cameras: {[c['camera_id'] for c in cameras]}")
    threads = []
    for cam in cameras:
        t = threading.Thread(
            target=process_camera, args=(cam,), name=f"yolo-{cam['camera_id']}", daemon=True
        )
        t.start()
        threads.append(t)
        time.sleep(2)  # смещение старта — меньше пиковой нагрузки на CPU/GPU
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
