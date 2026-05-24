# Отчёт — модуль Д

## ML

- **Модель:** Prophet, версия `prophet-v1`
- **Гранулярность:** 30 мин, отдельная модель на `(camera_id, direction)`
- **Горизонт:** 30 мин вперёд
- **Метрики:** MAE, RMSE на hold-out 24 ч → поля `mae_holdout`, `rmse_holdout`
- **Публикация:** `gold_predictions` через `ml_forecast.py` / Airflow

## Дашборд Metabase

См. `metabase/dashboard_setup.md` — 4 блока метрик + фильтры.

## Демонстрация эксперту

1. Показать строку прогноза в ClickHouse.
2. Открыть дашборд: realtime vs batch vs forecast на одной оси времени.
3. Фильтр по `incident_type` на графике инцидентов.
