# Платформа аналитики транспортных данных (v3)

**Рабочая папка агента:** `~/data_analitick_3_11_v3`

## Расписание конкурса

См. **[CONTEST_SCHEDULE.md](CONTEST_SCHEDULE.md)** — не переходите в другие модули: в каждой папке уже есть копии нужных файлов.

| День | Модули | Запуск |
|------|--------|--------|
| 1 | А → Б | `module_A/scripts/run_module.sh` → `module_B/scripts/run_module.sh` |
| 2 | В → Г | `module_V/scripts/run_module.sh` → `module_G/scripts/run_module.sh` |
| 3 | Д → Е | `module_D/scripts/run_module.sh` → `module_E/scripts/run_module.sh` |

## Один раз из корня

```bash
cd ~/data_analitick_3_11_v3
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
ln -sf ../data_analitick_3_11/yolov8n.pt models/yolov8n.pt
```

## В каждом модуле

- **README.md** — что делать в этот день
- **CRITERIA_DEMO.md** — что показать эксперту (SQL, команды, строки кода)
- **scripts/run_module.sh** — запуск без поиска по проекту

Стек: YOLO · MinIO · PostgreSQL · ClickHouse · Kafka · Airflow · Metabase · draw.io
