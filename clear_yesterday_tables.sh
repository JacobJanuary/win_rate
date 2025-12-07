#!/bin/bash
# Скрипт очистки yesterday таблиц
# Использование: bash clear_yesterday_tables.sh

set -e

# Загрузить переменные из .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ Файл .env не найден!"
    exit 1
fi

# Проверить обязательные переменные
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ]; then
    echo "❌ Не все переменные заданы в .env файле!"
    echo "Требуются: DB_HOST, DB_PORT, DB_NAME, DB_USER"
    exit 1
fi

echo "🔴 ВНИМАНИЕ: Этот скрипт очистит все yesterday таблицы!"
echo ""
echo "Будут очищены:"
echo "  - optimization.yesterday_results"
echo "  - optimization.yesterday_signals"
echo "  - optimization.yesterday_candles"
echo ""
echo "База данных: $DB_HOST:$DB_PORT/$DB_NAME (пользователь: $DB_USER)"
echo ""
read -p "Продолжить? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Отменено"
    exit 0
fi

echo ""
echo "📊 Текущее состояние таблиц:"

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
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

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
TRUNCATE TABLE optimization.yesterday_results CASCADE;
TRUNCATE TABLE optimization.yesterday_signals CASCADE;
TRUNCATE TABLE optimization.yesterday_candles CASCADE;
EOF

echo ""
echo "✅ Таблицы очищены!"
echo ""
echo "📊 Проверка:"

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
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
