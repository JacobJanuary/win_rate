#!/bin/bash
# Скрипт очистки таблиц оптимизации
# Использование: bash clear_optimization_tables.sh

set -e

echo "🔴 ВНИМАНИЕ: Этот скрипт очистит все таблицы оптимизации!"
echo ""
echo "Будут очищены:"
echo "  - optimization.simulation_results"
echo "  - optimization.selected_signals"
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
    'selected_signals' as table_name, 
    COUNT(*) as count,
    pg_size_pretty(pg_total_relation_size('optimization.selected_signals')) as size
FROM optimization.selected_signals
UNION ALL
SELECT 
    'simulation_results', 
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('optimization.simulation_results'))
FROM optimization.simulation_results;
EOF

echo ""
read -p "Создать backup перед очисткой? (yes/no): " backup

if [ "$backup" = "yes" ]; then
    echo "📦 Создание backup..."
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    
    psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new << EOF
    CREATE TABLE optimization.selected_signals_backup_${TIMESTAMP} AS 
    SELECT * FROM optimization.selected_signals;
    
    CREATE TABLE optimization.simulation_results_backup_${TIMESTAMP} AS 
    SELECT * FROM optimization.simulation_results;
EOF
    
    echo "✅ Backup создан: selected_signals_backup_${TIMESTAMP}, simulation_results_backup_${TIMESTAMP}"
fi

echo ""
echo "🗑️  Очистка таблиц..."

psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new << 'EOF'
TRUNCATE TABLE optimization.simulation_results CASCADE;
TRUNCATE TABLE optimization.selected_signals CASCADE;
EOF

echo ""
echo "✅ Таблицы очищены!"
echo ""
echo "📊 Проверка:"

psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new << 'EOF'
SELECT 
    'selected_signals' as table_name, COUNT(*) as count 
FROM optimization.selected_signals
UNION ALL
SELECT 
    'simulation_results', COUNT(*) 
FROM optimization.simulation_results;
EOF

echo ""
echo "✅ Готово! Теперь можно запускать скрипты оптимизации."
echo ""
echo "Следующий шаг:"
echo "  cd optimization"
echo "  python3 extract_top_signals.py LONG BULL"
