-- Gold: потоковые витрины (модуль В). Копия фрагмента DWH.
CREATE DATABASE IF NOT EXISTS transport;

CREATE TABLE IF NOT EXISTS transport.streaming_metrics (
    camera_id       String,
    direction       String,
    timestamp       DateTime,
    avg_speed_kmh   Float32,
    intensity_per_min Float32,
    share_car       Float32,
    share_truck     Float32,
    share_bus       Float32,
    share_motorcycle Float32,
    total_detections_window UInt32
) ENGINE = MergeTree()
ORDER BY (camera_id, direction, timestamp);

CREATE TABLE IF NOT EXISTS transport.gold_incidents (
    incident_id     String,
    camera_id       String,
    direction       String,
    incident_type   String,
    timestamp       DateTime,
    track_id        UInt32,
    value           Float32,
    details         String,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (camera_id, timestamp, incident_type);
