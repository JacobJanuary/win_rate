#!/bin/bash
# Скрипт очистки yesterday таблиц
# Использование: bash clear_yesterday_tables.sh

set -e

echo "🔴 ВНИМАНИЕ: Этот скрипт очистит все yesterday таблицы!"
echo ""
echo "Будут очищены:"
echo "  - optimization.yesterday_results"
echo "  - optimization.yesterday_signals"
echo "  - optimization.yesterday_candles"
echo ""
read -p "Продолжить? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Отменено"
    exit 0
fi

echo ""
echo "📊 Текущее состояние таблиц:"

psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new << 'EOF'
SELECT 
    'yesterday_signals' as table_name, 
    COUNT(*) as count,
    pg_size_pretty(pg_total_relation_size('optimization.yesterday_signals')) as size
FROM optimization.yesterday_signals
UNION ALL
SELECT 
    'yesterday_results', 
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('optimization.yesterday_results'))
FROM optimization.yesterday_results
UNION ALL
SELECT 
    'yesterday_candles', 
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('optimization.yesterday_candles'))
FROM optimization.yesterday_candles;
EOF

echo ""
echo "🗑️  Очистка таблиц..."

psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new << 'EOF'
TRUNCATE TABLE optimization.yesterday_results CASCADE;
TRUNCATE TABLE optimization.yesterday_signals CASCADE;
TRUNCATE TABLE optimization.yesterday_candles CASCADE;
EOF

echo ""
echo "✅ Таблицы очищены!"
echo ""
echo "📊 Проверка:"

psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new << 'EOF'
SELECT 
    'yesterday_signals' as table_name, COUNT(*) as count 
FROM optimization.yesterday_signals
UNION ALL
SELECT 
    'yesterday_results', COUNT(*) 
FROM optimization.yesterday_results
UNION ALL
SELECT 
    'yesterday_candles', COUNT(*) 
FROM optimization.yesterday_candles;
EOF

echo ""
echo "✅ Готово! Теперь можно запускать yesterday analysis."
echo ""
echo "Следующий шаг:"
echo "  cd yesterday"
echo "  bash run_yesterday_analysis.sh"
