# Metabase — дашборд (модуль Д)

## Подключение ClickHouse

1. Admin → Databases → Add → ClickHouse
2. Host: `localhost`, Port: `9000` (native) или HTTP `8123`
3. Database: `transport`, user/password из `.env`

## Карточки

### 1. Realtime средняя скорость

```sql
SELECT timestamp AS time, avg_speed_kmh
FROM streaming_metrics
WHERE camera_id = {{camera_id}}
  AND direction = {{direction}}
ORDER BY timestamp DESC
LIMIT 200
```

Тип: Line, ось X = time.

### 2. Batch средняя скорость (30 мин)

```sql
SELECT window_start AS time, avg(avg_speed_kmh) AS batch_speed
FROM gold_traffic_aggregates
WHERE camera_id = {{camera_id}} AND direction = {{direction}}
GROUP BY window_start
ORDER BY window_start
```

### 3. Прогноз

```sql
SELECT prediction_time AS time, predicted_avg_speed_kmh AS forecast_speed
FROM gold_predictions
WHERE camera_id = {{camera_id}} AND direction = {{direction}}
ORDER BY prediction_time DESC
LIMIT 48
```

### 4. Опасные ситуации

```sql
SELECT timestamp AS time, incident_type, value
FROM gold_incidents
WHERE camera_id = {{camera_id}}
  AND direction = {{direction}}
  [[AND incident_type = {{incident_type}}]]
ORDER BY timestamp DESC
```

Тип: Bar или Line по time, series = incident_type.

## Фильтры дашборда

- `camera_id` — Text или Field Filter
- `direction` — Text (in / out / unknown)
- `incident_type` — speed_violation, dangerous_maneuver, congestion, truck_congestion

## Сквозной вид

Dashboard «Мониторинг транспорта»: 4 карточки + общие фильтры сверху.
