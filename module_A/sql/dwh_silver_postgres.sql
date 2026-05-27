-- Silver layer (PostgreSQL) — очищенные факты и справочники
-- Medallion: Silver | Kimball: измерения + факты событий
CREATE DATABASE silver;
\c silver;

-- [Справочник: Камеры] — критерий А.4
CREATE TABLE dim_cameras (
    camera_id       VARCHAR(20) PRIMARY KEY,
    location        VARCHAR(100),
    rtsp_url        VARCHAR(255),
    roi_coords      JSONB,
    perspective_matrix JSONB,
    speed_limit_kmh FLOAT DEFAULT 60.0,
    active          BOOLEAN DEFAULT true
);

-- [Справочник: Типы транспортных средств] — критерий А.4
CREATE TABLE dim_vehicle_types (
    vehicle_type_id INT PRIMARY KEY,
    type_name       VARCHAR(20) NOT NULL,
    description     VARCHAR(50)
);

INSERT INTO dim_vehicle_types VALUES
(2, 'car', 'Легковой автомобиль'),
(3, 'motorcycle', 'Мотоцикл'),
(5, 'bus', 'Автобус'),
(7, 'truck', 'Грузовик');

-- [Факт: детекция / трек] — ядро потока, связь с трекером
CREATE TABLE silver_detections (
    detection_id    UUID PRIMARY KEY,
    camera_id       VARCHAR(20) NOT NULL REFERENCES dim_cameras(camera_id),
    event_time      TIMESTAMP NOT NULL,
    frame_id        INT,
    track_id        INT,
    vehicle_type_id INT REFERENCES dim_vehicle_types(vehicle_type_id),
    bbox_x1         FLOAT, bbox_y1 FLOAT,
    bbox_x2         FLOAT, bbox_y2 FLOAT,
    confidence      FLOAT,
    speed_kmh       FLOAT,
    direction       VARCHAR(10),
    frame_path      VARCHAR(500),
    track_enter     TIMESTAMP,
    track_exit      TIMESTAMP,
    is_anomaly      BOOLEAN DEFAULT false,
    processed_at    TIMESTAMP DEFAULT now()
);

-- [Трекеры] — история перемещения по кадрам — критерий А.4
CREATE TABLE silver_track_history (
    history_id      BIGSERIAL PRIMARY KEY,
    camera_id       VARCHAR(20) NOT NULL,
    track_id        INT NOT NULL,
    event_time      TIMESTAMP NOT NULL,
    center_x        FLOAT,
    center_y        FLOAT,
    speed_kmh       FLOAT,
    direction       VARCHAR(10)
);

CREATE INDEX idx_silver_det_time ON silver_detections(camera_id, event_time);
CREATE INDEX idx_silver_det_track ON silver_detections(camera_id, track_id, event_time);
CREATE INDEX idx_track_hist ON silver_track_history(camera_id, track_id, event_time);

INSERT INTO dim_cameras (camera_id, location, rtsp_url, speed_limit_kmh) VALUES
('CAM-001', 'ул.', 'file:///', 60),
('CAM-002', 'пр.', 'file:///', 50)
ON CONFLICT (camera_id) DO NOTHING;
