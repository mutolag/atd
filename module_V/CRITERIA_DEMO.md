# Модуль В — демонстрация эксперту (День 2, утро)

Запуск: `./scripts/run_module.sh [init|stream|all]` из **`module_V/`**.

**Копии в этой папке (не ходить в А/Б):**
- `src/perspective_transform.py` — перспектива (скорость приходит из Kafka от YOLO)
- `config/cameras.yaml` — лимиты скорости
- `scripts/kafka_to_silver.py`, `sql/dwh_silver_postgres.sql`
- `sql/gold_streaming.sql` — таблицы Gold для потока

**Предусловие:** модуль Б пишет в Kafka (`raw_detections`). Либо снова запустить детектор из `module_B/`.

---

## В.1 Средняя скорость

| Критерий | Строка кода |
|----------|-------------|
| Расчёт avg | `src/streaming_processor.py:140-148` |
| Публикация в DWH | `src/streaming_processor.py:166-174` → `streaming_metrics` |

**ClickHouse:**
```sql
SELECT camera_id, direction, timestamp, avg_speed_kmh, total_detections_window
FROM transport.streaming_metrics
ORDER BY timestamp DESC LIMIT 10;
```

---

## В.2 Опасные ситуации

| Тип | Функция | Строки |
|-----|---------|--------|
| Превышение скорости | `detect_speed_violation` | `84-94` |
| Опасный маневр (угол + скорость) | `detect_dangerous_maneuver` | `95-118` |
| Критическая плотность | `detect_congestion` | `119-127` |
| Скопление грузовиков | `detect_truck_congestion` | `129-139` |
| Запись инцидента | `publish_incident` | `175-191` |

**ClickHouse:**
```sql
SELECT incident_type, count() AS c
FROM transport.gold_incidents
GROUP BY incident_type;

SELECT * FROM transport.gold_incidents
ORDER BY timestamp DESC LIMIT 10;
```

---

## В.3 Перспектива

| Критерий | Где |
|----------|-----|
| Метрическая скорость в Б | `src/perspective_transform.py` (копия из Б) |
| Использование в потоке | поле `speed_kmh` в Kafka, пороги в `streaming_processor.py:221-227` |

**Сказать эксперту:** скорость в км/ч после гомографии в модуле Б; модуль В применяет её к маневру и лимиту.

---

## В.4 Контроль качества

| Критерий | Строка кода |
|----------|-------------|
| Отсев аномалий | `validate_detection` `65-74`, `validate_metrics` `74-83` |
| Лог, не DWH | `log_anomaly` `58-63` → `../../logs/anomalies/{camera_id}.log` |

**Демо лога:**
```bash
tail -5 ../logs/anomalies/CAM-001.log
```

Показать, что при `speed > 150` строка в логе, а в `streaming_metrics` нет мусора.

---

## В.5 Отчёт

`REPORT.md`

---

## День 2 — модуль Г

```bash
cd ../module_G
./scripts/run_module.sh all
```
