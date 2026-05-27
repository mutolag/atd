import cv2
from ultralytics import YOLO
from datetime import datetime, timezone
import os
import boto3
import uuid
from kafka import KafkaProducer
import json
import numpy as np
import time

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)
    
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9001")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze-frames")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_DETECTIONS", "raw_detections")

client_minio = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
)
client_minio.head_bucket(Bucket=MINIO_BUCKET)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, cls=NumpyEncoder).encode("utf-8"),
    max_block_ms=10000,
)

def determine_direction(positions):
    """Направление in/out по векторам из cameras.yaml (конкурс: потоки наблюдения)."""
    if len(positions) < 2:
        return "unknown"
    _, x0, y0 = positions[-2]
    _, x1, y1 = positions[-1]
    dx, dy = x1 - x0, y1 - y0
    mag = (dx * dx + dy * dy) ** 0.5
    if mag < 3.0:
        return "unknown"
    in_v = [0, 1]
    out_v = [1, 0]
    dot_in = (dx * in_v[0] + dy * in_v[1]) / mag
    dot_out = (dx * out_v[0] + dy * out_v[1]) / mag
    if dot_in > 0.35 and dot_in >= dot_out:
        return "in"
    if dot_out > 0.35 and dot_out > dot_in:
        return "out"
    return "unknown"

VEHICLE_CLASSES = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
model = YOLO('/home/user/data_analitick_3_11_v3/models/yolov8n.pt')
source = '../data_analitick_3_11/video/cam1.mp4'
camera_id = 'CAM-001'
cap = cv2.VideoCapture(source)
frame_id = 0
while cap.isOpened():
    ret, frame = cap.read()
    results = model.track(
            frame, persist=True, classes=list(VEHICLE_CLASSES.keys()), verbose=False
        )
    frame1 = results[0].plot()
    frame_id += 1
    # отправка в minio
    if results and results[0].boxes is not None and len(results[0].boxes):
        now = datetime.now()
        key = f"{camera_id}/{now.strftime("%Y%m%d")}/frame_{now.strftime("%H%M%S")}_{now.microsecond // 1000:03d}.jpg"
        _, buf = cv2.imencode(".jpg", frame)
        client_minio.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.tobytes(), ContentType="image/jpeg")
        frame_path = f"s3://{MINIO_BUCKET}/{key}"

    # отправка в кафку
    for box in results[0].boxes:
        cls_id = int(box.cls[0])
        if cls_id not in VEHICLE_CLASSES:
            continue
        track_id = int(box.id[0]) if box.id is not None else -1
        x1, y1, x2, y2 = map(float, box.xyxy[0])
        cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
        conf = float(box.conf[0])
        st = dict()
        st['track_id'] = track_id
        now_ts = time.time()
        st["enter_time"] = datetime.now()
        st["positions"] = [(now_ts, cx, cy)]
        direction = determine_direction(st["positions"])
        st["direction"] = direction
        speed_kmh = 0.0

        payload = {
            "detection_id": str(uuid.uuid4()),
            "camera_id": camera_id,
            "speed_limit_kmh": 60,
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
        }

        producer.send(KAFKA_TOPIC, key=camera_id.encode(), value=payload)
        print(frame_id)
    # cv2.imshow('sdfghj', frame1)
    # if cv2.waitKey(1) & 0xFF == ord('q'):
    #     break
cap.release()


