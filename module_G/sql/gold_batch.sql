-- Gold: batch-витрины (модуль Г). Копия фрагмента DWH.
CREATE DATABASE IF NOT EXISTS transport;

CREATE TABLE IF NOT EXISTS transport.gold_traffic_aggregates (
    camera_id       String,
    direction       String,
    window_start    DateTime,
    window_end      DateTime,
    vehicle_type    String,
    total_count     UInt32,
    avg_speed_kmh   Float32,
    p25_speed       Float32,
    p75_speed       Float32,
    share_of_total  Float32,
    is_anomaly      Bool DEFAULT false,
    processed_at    DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (camera_id, direction, window_start, vehicle_type);

CREATE TABLE IF NOT EXISTS transport.gold_watermark (
    pipeline_name   String,
    last_event_time DateTime,
    updated_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY pipeline_name;

INSERT INTO transport.gold_watermark VALUES ('silver_to_gold_batch', toDateTime('1970-01-01 00:00:00'), now());
