# Модуль В — День 2 (утро)

**Только `module_V/`.** Внутри копии из Б/А: `perspective_transform.py`, `cameras.yaml`, `kafka_to_silver.py`, SQL Silver и Gold streaming.

## Запуск

Нужен поток в Kafka (запустите детектор из `module_B` в другом терминале или используйте записанный топик).

```bash
cd ~/data_analitick_3_11_v3/module_V
source ../.venv/bin/activate

./scripts/run_module.sh init     # таблицы CH
./scripts/run_module.sh stream   # потоковая аналитика

# с поднятием Silver consumer:
./scripts/run_module.sh all
```

## Демонстрация эксперту

**[CRITERIA_DEMO.md](CRITERIA_DEMO.md)** — метрики, 4 типа инцидентов, DQ-логи, запросы ClickHouse.

## День 2 (день)

```bash
cd ../module_G
./scripts/run_module.sh all
```
