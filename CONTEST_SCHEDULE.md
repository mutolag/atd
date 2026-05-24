# Расписание конкурса (3 дня)

Работайте **только в папке текущего модуля**. Всё нужное для дня уже скопировано внутрь модуля.

| День | Модули | Папки | С чего начать |
|------|--------|-------|----------------|
| **1** | А, Б | `module_A/`, `module_B/` | `module_A/scripts/run_module.sh` → затем `module_B/scripts/run_module.sh` |
| **2** | В, Г | `module_V/`, `module_G/` | `module_V/scripts/run_module.sh` → `module_G/scripts/run_module.sh` |
| **3** | Д, Е | `module_D/`, `module_E/` | `module_D/scripts/run_module.sh` → `module_E/scripts/run_module.sh` |

В каждой папке:

- **README.md** — запуск модуля
- **CRITERIA_DEMO.md** — что показать эксперту (команды, SQL, строки кода)
- **scripts/run_module.sh** — одна точка входа

Общее окружение один раз из корня: `python3 -m venv .venv`, `pip install -r requirements.txt`, `.env`.
