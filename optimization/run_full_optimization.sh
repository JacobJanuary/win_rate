#!/bin/bash
# Полный цикл оптимизации
# Запускает все этапы автоматически

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "🚀 Запуск полного цикла оптимизации"
echo "===================================="
echo ""
echo "Этапы:"
echo "  1. Извлечение сигналов (6 комбинаций)"
echo "  2. Загрузка свечей"
echo "  3. Симуляция сделок"
echo "  4. Агрегация результатов"
echo ""
echo "⏱️  Ожидаемое время: 4-7 часов"
echo ""
read -p "Продолжить? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Отменено"
    exit 0
fi

START_TIME=$(date +%s)

# Этап 1: Извлечение сигналов
echo ""
echo "================================================"
echo "📊 ЭТАП 1/4: Извлечение топ сигналов"
echo "================================================"
echo ""

combinations=("LONG BULL" "LONG BEAR" "LONG NEUTRAL" "SHORT BULL" "SHORT BEAR" "SHORT NEUTRAL")
total=${#combinations[@]}
current=0

for combo in "${combinations[@]}"; do
    current=$((current + 1))
    echo ""
    echo "[$current/$total] Обработка: $combo"
    echo "-----------------------------------"
    
    if python3 extract_top_signals.py $combo; then
        echo "✅ $combo - готово"
    else
        echo "❌ Ошибка при обработке $combo"
        exit 1
    fi
done

echo ""
echo "✅ Этап 1 завершён: Все сигналы извлечены"

# Этап 2: Загрузка свечей
echo ""
echo "================================================"
echo "📈 ЭТАП 2/4: Загрузка свечей с Binance"
echo "================================================"
echo ""

if python3 fetch_binance_candles.py; then
    echo "✅ Этап 2 завершён: Свечи загружены"
else
    echo "❌ Ошибка при загрузке свечей"
    exit 1
fi

# Этап 3: Симуляция сделок
echo ""
echo "================================================"
echo "🎯 ЭТАП 3/4: Симуляция сделок"
echo "================================================"
echo ""

if python3 simulate_trades.py; then
    echo "✅ Этап 3 завершён: Сделки симулированы"
else
    echo "❌ Ошибка при симуляции сделок"
    exit 1
fi

# Этап 4: Агрегация результатов
echo ""
echo "================================================"
echo "📊 ЭТАП 4/4: Агрегация результатов"
echo "================================================"
echo ""

if python3 aggregate_results.py; then
    echo "✅ Этап 4 завершён: Результаты агрегированы"
else
    echo "❌ Ошибка при агрегации результатов"
    exit 1
fi

# Финальная статистика
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))

echo ""
echo "================================================"
echo "🎉 ВСЕ ЭТАПЫ ЗАВЕРШЕНЫ!"
echo "================================================"
echo ""
echo "⏱️  Время выполнения: ${HOURS}ч ${MINUTES}мин"
echo ""
echo "📊 Проверка результатов:"
echo ""

# Проверка через psql
if [ -f ../.env ]; then
    export $(grep -v '^#' ../.env | xargs)
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
SELECT 
    'selected_signals' as table_name, COUNT(*) as count 
FROM optimization.selected_signals
UNION ALL
SELECT 'simulation_results', COUNT(*) FROM optimization.simulation_results
UNION ALL
SELECT 'best_parameters', COUNT(*) FROM optimization.best_parameters;
EOF
fi

echo ""
echo "✅ Готово! Можно запускать yesterday analysis или перезапускать WebSocket сервер."
echo ""
echo "Следующие шаги:"
echo "  cd ../yesterday && bash run_yesterday_analysis.sh"
echo "  sudo systemctl restart optimized-signal-server"
