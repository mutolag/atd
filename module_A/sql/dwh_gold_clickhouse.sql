-- Gold layer (ClickHouse) — аналитические витрины (OLAP)
CREATE DATABASE IF NOT EXISTS transport;

-- [Витрина 1: потоковые метрики realtime] — модуль В
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

-- [Витрина 2: batch-агрегаты 30 мин] — модуль Г
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

-- [Факт: опасная ситуация] — критерий А.4
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

-- [Факт: прогноз средней скорости] — критерий А.4, модуль Д
CREATE TABLE IF NOT EXISTS transport.gold_predictions (
    camera_id       String,
    direction       String,
    prediction_time DateTime,
    forecast_horizon_minutes UInt8,
    predicted_avg_speed_kmh Float32,
    prediction_interval_lower Float32,
    prediction_interval_upper Float32,
    model_version   String,
    mae_holdout     Float32,
    rmse_holdout    Float32,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (camera_id, direction, prediction_time);

-- Аудит качества данных
CREATE TABLE IF NOT EXISTS transport.gold_data_quality_audit (
    check_id        String,
    check_type      String,
    source_layer    String,
    table_name      String,
    check_time      DateTime,
    camera_id       String,
    result          String,
    metric_value    Float32,
    threshold       Float32,
    details         String
) ENGINE = MergeTree()
ORDER BY (check_time, table_name);

-- Watermark batch (опоздавшие данные) — модуль Г
CREATE TABLE IF NOT EXISTS transport.gold_watermark (
    pipeline_name   String,
    last_event_time DateTime,
    updated_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY pipeline_name;

INSERT INTO transport.gold_watermark VALUES ('silver_to_gold_batch', toDateTime('1970-01-01 00:00:00'), now());
