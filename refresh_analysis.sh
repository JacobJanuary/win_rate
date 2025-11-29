#!/bin/bash

# --- Настройки подключения к базе данных ---
DB_HOST="localhost"
DB_USER="elcrypto"
DB_NAME="fox_crypto_new"

# --- Начало выполнения скрипта ---
echo "▶️  Запуск скрипта обновления аналитических таблиц..."

# Выполнение SQL-команд с помощью psql и "here document"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF

-- 3. Перезаполнение таблиц с новой логикой (v7.0 - unified periods)
SELECT fas_v2.save_pattern_analysis('weekly');
SELECT fas_v2.save_pattern_analysis('monthly');

-- Для пересчета за последние 30 дней
SELECT * FROM fas_v2.recalculate_win_rates_for_period();
EOF

# --- Проверка результата и завершение ---
if [ $? -eq 0 ]; then
  echo "✅  Скрипт успешно выполнен."
else
  echo "❌  Во время выполнения скрипта произошла ошибка."
fi
