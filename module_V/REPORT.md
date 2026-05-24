# Отчёт — модуль В

## Потоковый контур

Consumer `raw_detections` → валидация → буфер по `(camera_id, direction)` → метрики в `transport.streaming_metrics`, инциденты в `transport.gold_incidents`.

## Опасные ситуации

1. **speed_violation** — превышение лимита из payload (из yaml камеры).
2. **dangerous_maneuver** — изменение угла движения центра bbox ≥45° при скорости ≥40 км/ч.
3. **congestion** — критическая плотность (≥25 объектов).
4. **truck_congestion** — ≥8 грузовиков.

## Перспектива

Метрическая скорость рассчитывается в модуле Б (`PerspectiveTransformer`). Модуль В использует `speed_kmh` для порогов и средних — единый физический смысл для маневра и превышения.

## Контроль качества

- `validate_detection` / `validate_metrics` — отсев перед INSERT.
- `log_anomaly(camera_id, ...)` — файл на камеру в `logs/anomalies/`.

## Интеграция

ClickHouse Gold инициализирован в модуле А (`dwh_gold_clickhouse.sql`).
