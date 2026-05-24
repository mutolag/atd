# Отчёт — модуль Б

## Выбор технологии

**YOLOv8 (Ultralytics) + ByteTrack** — единый стек inference и трекинга на CPU/GPU площадки; классы COCO покрывают легковые, грузовые, автобусы, мото. Альтернатива Detectron2 избыточна для 4 классов и хуже укладывается в 3 часа.

## Реализация

| Требование | Реализация |
|------------|------------|
| Подключение к потоку | `cv2.VideoCapture`, RTSP/file из yaml |
| Детекция ТС | YOLO classes 2,3,5,7 |
| Типы | `vehicle_type_id` + справочник Silver |
| Направление | `determine_direction()` + yaml vectors |
| Трекинг | ByteTrack, `track_id`, `silver_track_history` |
| enter/exit | `track_enter`, `track_exit` в payload |
| Кадры | MinIO, `frame_path` |
| Масштабирование | `cameras.yaml`, thread на камеру |

## Визуальное подтверждение

`python module_B/src/visual_demo.py` → `output/demo_detection_*.jpg`.

## Перспектива

Скорость в км/ч считается в `perspective_transform.py` (используется модулем В через поле `speed_kmh` в Kafka).

## Демо одной камеры

`export ACTIVE_CAMERAS=CAM-001` — снижает нагрузку на слабом ПК.
