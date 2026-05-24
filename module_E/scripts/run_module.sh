#!/usr/bin/env bash
# Модуль Е — День 3. Чек-лист демонстрации всей системы.
set -euo pipefail
MODULE_E="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$MODULE_E/.." && pwd)"

cat "$MODULE_E/CRITERIA_DEMO.md"
echo ""
echo "Презентация: $MODULE_E/presentation/OUTLINE.md"
echo "Диаграммы: $ROOT/module_A/diagrams/"
