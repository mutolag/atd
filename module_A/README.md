# Модуль А — День 1 (утро)

**Не открывайте другие модули.** Всё для сдачи А — здесь.

## Запуск

```bash
cd ~/data_analitick_3_11_v3/module_A
../.venv/bin/activate   # или source ../.venv/bin/activate из корня
./scripts/run_module.sh
```

## Демонстрация эксперту

Откройте **[CRITERIA_DEMO.md](CRITERIA_DEMO.md)** — пошагово: диаграммы, SQL, Kafka, строки в коде.

## Содержимое папки

| Файл | Назначение |
|------|------------|
| `requirements.md` | FN/NFR |
| `tech_stack.md` | Стек + обоснование |
| `sql/` | Silver + Gold DDL |
| `diagrams/` | draw.io |
| `scripts/kafka_to_silver.py` | Kafka → PostgreSQL |
| `REPORT.md` | Отчёт |

## Следующий шаг (тот же день)

```bash
cd ../module_B
./scripts/run_module.sh all
```
