#!/bin/bash
# Clear Win Strategy Tables
# Removes all data from winning combinations optimization tables

set -e

echo "🧹 Clearing Win Strategy Tables"
echo "================================"
echo ""
echo "⚠️  This will delete ALL data from:"
echo "   - optimization.combination_simulations"
echo "   - optimization.combination_candles"
echo "   - optimization.combination_signals"
echo "   - optimization.combination_best_parameters"
echo "   - optimization.winning_combinations"
echo ""
read -p "Continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cancelled"
    exit 0
fi

# Load database credentials
if [ -f ../.env ]; then
    export $(grep -v '^#' ../.env | xargs)
else
    echo "❌ .env file not found"
    exit 1
fi

echo ""
echo "Clearing tables..."

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
-- Delete in correct order (respecting foreign keys)
DELETE FROM optimization.combination_simulations;
DELETE FROM optimization.combination_best_parameters;
DELETE FROM optimization.combination_candles;
DELETE FROM optimization.combination_signals;
DELETE FROM optimization.winning_combinations;

-- Show counts
SELECT 
    'combination_simulations' as table_name, COUNT(*) as count 
FROM optimization.combination_simulations
UNION ALL
SELECT 'combination_candles', COUNT(*) FROM optimization.combination_candles
UNION ALL
SELECT 'combination_signals', COUNT(*) FROM optimization.combination_signals
UNION ALL
SELECT 'combination_best_parameters', COUNT(*) FROM optimization.combination_best_parameters
UNION ALL
SELECT 'winning_combinations', COUNT(*) FROM optimization.winning_combinations;
EOF

echo ""
echo "✅ All tables cleared!"
