# Модуль А — День 1 (утро)

**Не открывайте другие модули** для сдачи А — всё необходимое здесь.

## Главные артефакты для экспертов

| Файл | Назначение |
|------|------------|
| **[REPORT.ipynb](REPORT.ipynb)** | **Главный отчёт** (ноутбук + места для скриншотов) |
| **[REPORT.md](REPORT.md)** | Тот же отчёт в Markdown |
| **[CRITERIA_DEMO.md](CRITERIA_DEMO.md)** | Чек-лист демонстрации по новым критериям |
| **[tech_stack.md](tech_stack.md)** | Стек, обоснование, подход Kimball+Medallion |
| **[diagrams/00_platform_architecture.drawio](diagrams/00_platform_architecture.drawio)** | **Единая** архитектурная диаграмма (А.3) |

## Запуск

```bash
cd ~/data_analitick_3_11_v3/module_A
source ../.venv/bin/activate
./scripts/run_module.sh
```

## Диаграмма

1. Откройте `diagrams/00_platform_architecture.drawio` в [draw.io](https://app.diagrams.net/).
2. Для отчёта/презентации: **File → Export as → PNG** → сохраните как `diagrams/00_platform_architecture.png`.

Схемы `01_system_architecture.drawio` и `02_data_flow.drawio` — архив, для сдачи не требуются.

## Структура папки

```
module_A/
├── REPORT.md              ← отчёт для судей
├── CRITERIA_DEMO.md       ← что показать на РМ
├── tech_stack.md          ← стек + DWH (критерий А.2 «д»)
├── requirements.md        ← приложение к отчёту
├── sql/                   ← DDL + schema_mapping.md
├── scripts/               ← init, kafka_to_silver, run_module.sh
└── diagrams/
    └── 00_platform_architecture.drawio
```

## Следующий шаг (день 1)

```bash
cd ../module_B
./scripts/run_module.sh all
```
