# Формат сообщения Kafka `raw_detections`

Копия контракта модуля Б (для модуля В без открытия `module_B/`).

Поля, используемые потоковым процессором (`src/streaming_processor.py`):

| Поле | Тип | Назначение |
|------|-----|------------|
| camera_id | string | Камера |
| direction | string | in / out / unknown |
| speed_kmh | float | Скорость после перспективы (модуль Б) |
| speed_limit_kmh | float | Лимит из cameras.yaml |
| track_id | int | Трек ByteTrack |
| vehicle_type_id | int | 2,3,5,7 |
| bbox_x1..y2 | float | Для маневра (угол траектории) |
| timestamp | ISO | Время кадра |

Перспектива: см. копию `src/perspective_transform.py` в этой папке.
