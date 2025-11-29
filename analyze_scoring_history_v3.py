#!/usr/bin/env python3
"""
Улучшенный скрипт для анализа исторических данных скоринга
Рассчитывает результаты одновременно для LONG и SHORT позиций
Version: 7.0 - UNIFIED PERIODS + DETERMINISTIC SEED:
  ✅ #1: Корректный расчет max_drawdown от пика
  ✅ #2: Реалистичная цена входа (LONG ближе к high, SHORT ближе к low)
  ✅ #3: Статистический подход при одновременном срабатывании TP/SL (50/50)
  ✅ #4: Валидация данных (проверка цен, аномалий)
  ✅ #5: Устранена SQL injection уязвимость
  ✅ #6: Исправлена транзакционная целостность
  ✅ #7: Детерминированный random seed на уровне сигнала (v7.0)
  ✅ #8: Добавлены timeout'ы для БД
  ✅ #9: Улучшена обработка исключений
  ✅ #10: Добавлены индексы для быстрых запросов
  ✅ #11: Защита от деления на ноль
  ✅ #12: Унифицированные периоды с SQL функциями (v7.0)

Обрабатывает сигналы за период, согласованный с SQL функциями анализа паттернов

⚠️ ВРЕМЕННОЕ РЕШЕНИЕ:
Скрипт использует fas_v2.market_data_aggregated вместо fas_v2.market_data_aggregated
для получения рыночных данных.

ПРИЧИНА:
В fas_v2.market_data_aggregated отсутствуют данные для 520 из 630 торговых пар,
что приводит к потере 82% сигналов. Данные присутствуют в старой схеме fas_v2.

TODO:
После завершения миграции всех данных в fas_v2.market_data_aggregated необходимо:
1. Заменить fas_v2.market_data_aggregated на fas_v2.market_data_aggregated в методах:
   - get_entry_price() (строка ~264)
   - analyze_signal_both_directions() (строка ~615)
2. Удалить эти комментарии с предупреждениями
"""

import os
import sys
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import time
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import json
from pathlib import Path
from dotenv import load_dotenv
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analyze_scoring_history_v2.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class AnalysisConfig:
    """Конфигурация параметров анализа"""
    tp_percent: float = 3.0
    sl_percent: float = 3.0
    position_size: float = 100.0
    leverage: int = 10
    analysis_hours: int = 3  # Окно анализа в часах (1, 3, 6, 12, 24)
    entry_delay_minutes: int = 15  # Оставляем 15 минут
    batch_size: int = 10000
    save_batch_size: int = 100


@dataclass
class TradeResult:
    """Результат торговли для одного направления"""
    direction: str  # 'LONG' или 'SHORT'
    entry_price: float
    best_price: float
    worst_price: float
    close_price: float
    is_closed: bool
    close_reason: str
    is_win: Optional[bool]
    close_time: Optional[datetime]
    hours_to_close: Optional[float]
    pnl_percent: float
    pnl_usd: float
    max_potential_profit_percent: float
    max_potential_profit_usd: float
    max_drawdown_percent: float
    max_drawdown_usd: float
    absolute_max_price: float
    absolute_min_price: float
    time_to_max_hours: float
    time_to_min_hours: float


class ImprovedScoringAnalyzer:
    def __init__(self, config_path: str = "config.json"):
        """
        Инициализация с загрузкой конфигурации из файла
        """
        self.config = AnalysisConfig()
        self.db_config = self._load_db_config(config_path)
        self.conn = None
        self.processed_count = 0
        self.error_count = 0
        self.new_signals_count = 0
        self.skipped_count = 0

        # ✅ УДАЛЕНО: Глобальный random seed (теперь индивидуальный для каждого сигнала)

    def _load_db_config(self, config_path: str) -> dict:
        """
        Загрузка конфигурации БД.
        Приоритет: config.json > переменные окружения (включая .env).
        """
        config_file = Path(config_path)

        if config_file.exists():
            logger.info(f"Загрузка конфигурации из {config_path}...")
            with open(config_file, 'r') as f:
                config = json.load(f)
            db_conf = config.get('database')
            if db_conf and db_conf.get('host') and db_conf.get('user'):
                logger.info("✅ Конфигурация БД успешно загружена из config.json.")
                return db_conf

        logger.info("Конфигурация в config.json не найдена, переход к переменным окружения (.env).")
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'dbname': os.getenv('DB_NAME', 'fox_crypto'),
            'user': os.getenv('DB_USER', 'elcrypto'),
            'password': os.getenv('DB_PASSWORD')
        }

    def connect(self):
        """Подключение к БД с поддержкой .pgpass"""
        try:
            conn_parts = [
                f"host={self.db_config.get('host', 'localhost')}",
                f"port={self.db_config.get('port', 5432)}",
                f"dbname={self.db_config.get('dbname')}",
                f"user={self.db_config.get('user')}"
            ]

            password = self.db_config.get('password')
            if password:
                conn_parts.append(f"password={password}")

            conn_string = " ".join(conn_parts)

            # ✅ ДОБАВЛЕНО: Таймауты для предотвращения зависаний
            self.conn = psycopg.connect(
                conn_string, 
                row_factory=dict_row,
                connect_timeout=30,  # Таймаут подключения 30 сек
                options='-c statement_timeout=300000'  # Таймаут запроса 5 минут
            )
            logger.info("✅ Успешное подключение к БД")

            self._ensure_results_table_exists()

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise

    def _ensure_results_table_exists(self):
        """Создание таблицы результатов если её не существует"""
        create_table_query = """
            CREATE TABLE IF NOT EXISTS web.scoring_history_results_v2 (
                id SERIAL PRIMARY KEY,
                scoring_history_id INTEGER NOT NULL,
                signal_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                pair_symbol VARCHAR(50) NOT NULL,
                trading_pair_id INTEGER NOT NULL,
                market_regime VARCHAR(50),
                total_score DECIMAL(10,2),
                indicator_score DECIMAL(10,2),
                pattern_score DECIMAL(10,2),
                combination_score DECIMAL(10,2),
                signal_type VARCHAR(10) NOT NULL, -- 'LONG' или 'SHORT'
                entry_price DECIMAL(20,8),
                best_price DECIMAL(20,8),
                worst_price DECIMAL(20,8),
                close_price DECIMAL(20,8),
                is_closed BOOLEAN DEFAULT FALSE,
                close_reason VARCHAR(50),
                is_win BOOLEAN,
                close_time TIMESTAMP WITH TIME ZONE,
                hours_to_close DECIMAL(10,2),
                pnl_percent DECIMAL(10,4),
                pnl_usd DECIMAL(15,2),
                max_potential_profit_percent DECIMAL(10,4),
                max_potential_profit_usd DECIMAL(15,2),
                max_drawdown_percent DECIMAL(10,4),
                max_drawdown_usd DECIMAL(15,2),
                tp_percent DECIMAL(5,2),
                sl_percent DECIMAL(5,2),
                position_size DECIMAL(10,2),
                leverage INTEGER,
                analysis_end_time TIMESTAMP WITH TIME ZONE,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(scoring_history_id, signal_type)
            );

            CREATE INDEX IF NOT EXISTS idx_scoring_results_timestamp
                ON web.scoring_history_results_v2(signal_timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_scoring_results_pair
                ON web.scoring_history_results_v2(trading_pair_id);
            CREATE INDEX IF NOT EXISTS idx_scoring_results_signal_type
                ON web.scoring_history_results_v2(signal_type);
            
            -- ✅ ДОБАВЛЕНО: Индекс для быстрой проверки NOT EXISTS в get_unprocessed_signals
            CREATE INDEX IF NOT EXISTS idx_scoring_results_history_id
                ON web.scoring_history_results_v2(scoring_history_id);
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(create_table_query)
                self.conn.commit()
                logger.info("✅ Таблица web.scoring_history_results_v2 готова к использованию")
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы результатов: {e}")
            raise

    def disconnect(self):
        """Отключение от БД"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 Отключение от БД")

    @staticmethod
    def get_signal_seed(scoring_history_id: int) -> int:
        """
        Генерация детерминированного seed для сигнала

        Args:
            scoring_history_id: ID сигнала из scoring_history

        Returns:
            int: Детерминированный seed для random

        Обеспечивает:
            - Детерминированность: один ID = один seed
            - Уникальность: разные ID = разные seed
            - Воспроизводимость: повторный вызов с тем же ID = тот же seed
        """
        import hashlib

        # Используем строку с префиксом для предотвращения коллизий
        seed_string = f"scoring_history_id_{scoring_history_id}"

        # hash() в Python 3 использует рандомизацию (PYTHONHASHSEED)
        # hashlib.sha256() всегда детерминирован
        hash_bytes = hashlib.sha256(seed_string.encode('utf-8')).digest()
        seed = int.from_bytes(hash_bytes[:8], byteorder='big')

        # Ограничиваем seed размером 32-bit int (требование random.seed)
        seed = seed % (2**32)

        return seed

    def get_unprocessed_signals(self, batch_size: int = 10000) -> List[Dict]:
        """
        Получение пакета необработанных сигналов
        Использует согласованный период с SQL функциями анализа паттернов
        """
        # Получаем период через SQL функцию для консистентности
        period_query = """
            SELECT
                period_start,
                period_end,
                days_count
            FROM fas_v2.get_analysis_period('monthly')
        """

        with self.conn.cursor() as cur:
            cur.execute(period_query)
            period_info = cur.fetchone()

        period_start = period_info['period_start']
        period_end = period_info['period_end']

        logger.info(f"📅 Analysis period: {period_start.date()} to {period_end.date()} ({period_info['days_count']} days)")

        query = """
            SELECT
                sh.id as scoring_history_id,
                sh.timestamp as signal_timestamp,
                sh.trading_pair_id,
                sh.pair_symbol,
                sh.total_score,
                sh.indicator_score,
                sh.pattern_score,
                sh.combination_score,
                mr.regime as market_regime
            FROM fas_v2.scoring_history sh
            LEFT JOIN LATERAL (
                SELECT regime
                FROM fas_v2.market_regime mr
                WHERE mr.timestamp <= sh.timestamp
                    AND mr.timeframe = '4h'
                ORDER BY mr.timestamp DESC
                LIMIT 1
            ) mr ON true
            WHERE sh.timestamp >= %s
                AND sh.timestamp <= %s
                AND NOT EXISTS (
                    SELECT 1 FROM web.scoring_history_results_v2 shr
                    WHERE shr.scoring_history_id = sh.id
                )
            ORDER BY sh.timestamp ASC
            LIMIT %s
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (period_start, period_end, batch_size))
            signals = cur.fetchall()

        return signals

    def get_entry_price(self, trading_pair_id: int, signal_time: datetime,
                        direction: str) -> Optional[Dict]:
        """
        Получение цены входа для указанного направления
        ✅ ИСПРАВЛЕНО: Реалистичная цена входа с учетом направления
        - LONG входит ближе к high (75% от диапазона) - хуже для трейдера
        - SHORT входит ближе к low (25% от диапазона) - хуже для трейдера
        Теперь работаем с 5-минутными свечами

        ⚠️ ВРЕМЕННОЕ РЕШЕНИЕ: Используем fas_v2.market_data_aggregated вместо fas_v2
        ПРИЧИНА: В fas_v2.market_data_aggregated отсутствуют данные для 520 из 630 торговых пар
        TODO: Вернуть использование fas_v2.market_data_aggregated после полной миграции данных
        """
        entry_time = signal_time + timedelta(minutes=self.config.entry_delay_minutes)

        query = """
            SELECT
                timestamp,
                close_price,
                high_price,
                low_price
            FROM fas_v2.market_data_aggregated
            WHERE trading_pair_id = %s
                AND timeframe = '5m'
                AND timestamp >= %s
            ORDER BY timestamp ASC
            LIMIT 1
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (trading_pair_id, entry_time))
                result = cur.fetchone()

            if result:
                high_price = float(result['high_price'])
                low_price = float(result['low_price'])
                
                # ✅ ДОБАВЛЕНО: Валидация данных
                if high_price <= 0 or low_price <= 0:
                    logger.error(
                        f"Некорректные цены для {trading_pair_id}: "
                        f"high={high_price}, low={low_price}"
                    )
                    return None
                
                if high_price < low_price:
                    logger.error(
                        f"High < Low для {trading_pair_id}: "
                        f"high={high_price}, low={low_price}"
                    )
                    return None
                
                # Проверка на аномальный спред (больше 50% за 5 минут - вероятно ошибка)
                spread_percent = (high_price - low_price) / low_price
                if spread_percent > 0.5:
                    logger.warning(
                        f"Аномальный спред {spread_percent*100:.1f}% "
                        f"для {trading_pair_id}, пропускаем"
                    )
                    return None
                
                if direction == 'LONG':
                    # Для LONG берем 75% от диапазона (ближе к high) - хуже для трейдера
                    entry_price = low_price + (high_price - low_price) * 0.75
                else:  # SHORT
                    # Для SHORT берем 25% от диапазона (ближе к low) - хуже для трейдера
                    entry_price = low_price + (high_price - low_price) * 0.25

                return {
                    'entry_price': entry_price,
                    'entry_time': result['timestamp']
                }
            return None

        except Exception as e:
            logger.error(f"❌ Ошибка получения цены входа: {e}")
            return None

    def calculate_trade_result(self, direction: str, entry_price: float,
                               history: List[Dict], actual_entry_time: datetime,
                               scoring_history_id: int) -> TradeResult:
        """
        Универсальный расчет результата торговли для указанного направления

        Args:
            direction: 'LONG' или 'SHORT'
            entry_price: Цена входа
            history: История свечей
            actual_entry_time: Время входа
            scoring_history_id: ID сигнала (для детерминированного random seed)

        Returns:
            TradeResult: Результат торговли

        ✅ ИСПРАВЛЕНО: Корректный расчет max_drawdown от пика
        ✅ ИСПРАВЛЕНО: Детерминированный random seed на уровне сигнала
        """
        tp_percent = self.config.tp_percent
        sl_percent = self.config.sl_percent
        position_size = self.config.position_size
        leverage = self.config.leverage

        # ✅ НОВОЕ: Устанавливаем детерминированный seed для этого сигнала
        signal_seed = self.get_signal_seed(scoring_history_id)
        random.seed(signal_seed)

        # Расчет уровней TP и SL
        if direction == 'LONG':
            tp_price = entry_price * (1 + tp_percent / 100)
            sl_price = entry_price * (1 - sl_percent / 100)
        else:  # SHORT
            tp_price = entry_price * (1 - tp_percent / 100)
            sl_price = entry_price * (1 + sl_percent / 100)

        # Инициализация переменных
        is_closed = False
        close_reason = None
        close_price = None
        close_time = None
        hours_to_close = None
        is_win = None

        # Переменные для отслеживания экстремумов
        absolute_max_price = entry_price
        absolute_min_price = entry_price
        time_to_max = 0
        time_to_min = 0

        # Переменные для трекинга максимальной просадки
        running_best_price = entry_price
        max_drawdown_from_peak = 0

        # Анализ истории цен
        for i, candle in enumerate(history):
            current_time = candle['timestamp']
            hours_passed = (current_time - actual_entry_time).total_seconds() / 3600

            high_price = float(candle['high_price'])
            low_price = float(candle['low_price'])

            # Обновляем абсолютные экстремумы
            if high_price > absolute_max_price:
                absolute_max_price = high_price
                time_to_max = hours_passed

            if low_price < absolute_min_price:
                absolute_min_price = low_price
                time_to_min = hours_passed

            # Проверяем закрытие позиции (только если еще не закрыта)
            if not is_closed:
                if direction == 'LONG':
                    # Проверяем, достигнуты ли оба уровня в одной свече
                    sl_hit = low_price <= sl_price
                    tp_hit = high_price >= tp_price

                    if sl_hit and tp_hit:
                        # Оба уровня достигнуты - используем статистический подход (50/50)
                        # Это честнее, чем всегда выбирать SL
                        hit_sl_first = random.choice([True, False])
                        is_closed = True
                        close_time = current_time
                        hours_to_close = hours_passed
                        
                        if hit_sl_first:
                            close_reason = 'stop_loss'
                            is_win = False
                            close_price = sl_price
                        else:
                            close_reason = 'take_profit'
                            is_win = True
                            close_price = tp_price
                            
                    elif sl_hit:
                        is_closed = True
                        close_reason = 'stop_loss'
                        is_win = False
                        close_price = sl_price
                        close_time = current_time
                        hours_to_close = hours_passed
                    elif tp_hit:
                        is_closed = True
                        close_reason = 'take_profit'
                        is_win = True
                        close_price = tp_price
                        close_time = current_time
                        hours_to_close = hours_passed

                else:  # SHORT
                    # Проверяем, достигнуты ли оба уровня в одной свече
                    sl_hit = high_price >= sl_price
                    tp_hit = low_price <= tp_price

                    if sl_hit and tp_hit:
                        # Оба уровня достигнуты - используем статистический подход (50/50)
                        # Это честнее, чем всегда выбирать SL
                        hit_sl_first = random.choice([True, False])
                        is_closed = True
                        close_time = current_time
                        hours_to_close = hours_passed
                        
                        if hit_sl_first:
                            close_reason = 'stop_loss'
                            is_win = False
                            close_price = sl_price
                        else:
                            close_reason = 'take_profit'
                            is_win = True
                            close_price = tp_price
                            
                    elif sl_hit:
                        is_closed = True
                        close_reason = 'stop_loss'
                        is_win = False
                        close_price = sl_price
                        close_time = current_time
                        hours_to_close = hours_passed
                    elif tp_hit:
                        is_closed = True
                        close_reason = 'take_profit'
                        is_win = True
                        close_price = tp_price
                        close_time = current_time
                        hours_to_close = hours_passed

            # Обновляем running best и считаем просадку
            if direction == 'LONG':
                if high_price > running_best_price:
                    running_best_price = high_price
                
                # ✅ ДОБАВЛЕНО: Защита от деления на ноль
                if running_best_price > 0:
                    current_drawdown = ((running_best_price - low_price) / running_best_price) * 100
                    if current_drawdown > max_drawdown_from_peak:
                        max_drawdown_from_peak = current_drawdown
            else:  # SHORT
                if low_price < running_best_price:
                    running_best_price = low_price
                
                # ✅ ДОБАВЛЕНО: Защита от деления на ноль
                if running_best_price > 0:
                    current_drawdown = ((high_price - running_best_price) / running_best_price) * 100
                    if current_drawdown > max_drawdown_from_peak:
                        max_drawdown_from_peak = current_drawdown

        # Если не закрылась за заданное время анализа
        if not is_closed:
            is_closed = True
            close_reason = 'timeout'
            is_win = None
            close_price = float(history[-1]['close_price'])
            close_time = history[-1]['timestamp']
            hours_to_close = float(self.config.analysis_hours)

        # ✅ ИСПРАВЛЕНО: Расчет финансовых показателей
        if direction == 'LONG':
            # Максимальный потенциальный профит для LONG
            max_potential_profit_percent = ((absolute_max_price - entry_price) / entry_price) * 100
            max_potential_profit_usd = position_size * leverage * (max_potential_profit_percent / 100)
            
            # ✅ ИСПРАВЛЕНО: Максимальная просадка от пика
            max_drawdown_usd = position_size * leverage * (max_drawdown_from_peak / 100)

            # Фактический P&L
            final_pnl_percent = ((close_price - entry_price) / entry_price) * 100
            best_price = absolute_max_price
            worst_price = absolute_min_price

        else:  # SHORT
            # Максимальный потенциальный профит для SHORT
            max_potential_profit_percent = ((entry_price - absolute_min_price) / entry_price) * 100
            max_potential_profit_usd = position_size * leverage * (max_potential_profit_percent / 100)
            
            # ✅ ИСПРАВЛЕНО: Максимальная просадка от пика
            max_drawdown_usd = position_size * leverage * (max_drawdown_from_peak / 100)

            # Фактический P&L
            final_pnl_percent = ((entry_price - close_price) / entry_price) * 100
            best_price = absolute_min_price
            worst_price = absolute_max_price

        final_pnl_usd = position_size * leverage * (final_pnl_percent / 100)

        return TradeResult(
            direction=direction,
            entry_price=entry_price,
            best_price=best_price,
            worst_price=worst_price,
            close_price=close_price,
            is_closed=is_closed,
            close_reason=close_reason,
            is_win=is_win,
            close_time=close_time,
            hours_to_close=hours_to_close,
            pnl_percent=final_pnl_percent,
            pnl_usd=final_pnl_usd,
            max_potential_profit_percent=max_potential_profit_percent,
            max_potential_profit_usd=max_potential_profit_usd,
            max_drawdown_percent=max_drawdown_from_peak,  # ✅ ИСПРАВЛЕНО
            max_drawdown_usd=max_drawdown_usd,           # ✅ ИСПРАВЛЕНО
            absolute_max_price=absolute_max_price,
            absolute_min_price=absolute_min_price,
            time_to_max_hours=time_to_max,
            time_to_min_hours=time_to_min
        )

    def create_no_data_result(self, signal: Dict, direction: str, reason: str) -> Dict:
        """
        Создает запись для сигналов без данных, чтобы они не обрабатывались повторно
        """
        return {
            'scoring_history_id': signal['scoring_history_id'],
            'signal_timestamp': signal['signal_timestamp'],
            'pair_symbol': signal['pair_symbol'],
            'trading_pair_id': signal['trading_pair_id'],
            'market_regime': signal['market_regime'],
            'total_score': float(signal['total_score']),
            'indicator_score': float(signal['indicator_score']),
            'pattern_score': float(signal['pattern_score']),
            'combination_score': float(signal.get('combination_score', 0)),
            'signal_type': direction,
            'entry_price': None,
            'best_price': None,
            'worst_price': None,
            'close_price': None,
            'is_closed': False,
            'close_reason': reason,  # 'no_entry_price' или 'insufficient_history'
            'is_win': None,
            'close_time': None,
            'hours_to_close': None,
            'pnl_percent': 0,
            'pnl_usd': 0,
            'max_potential_profit_percent': 0,
            'max_potential_profit_usd': 0,
            'max_drawdown_percent': 0,
            'max_drawdown_usd': 0,
            'tp_percent': self.config.tp_percent,
            'sl_percent': self.config.sl_percent,
            'position_size': self.config.position_size,
            'leverage': self.config.leverage,
            'analysis_end_time': signal['signal_timestamp'] + timedelta(hours=self.config.analysis_hours)
        }

    def analyze_signal_both_directions(self, signal: Dict) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        Анализ сигнала для обоих направлений (LONG и SHORT)
        ✅ ИСПРАВЛЕНО: Разные цены входа для LONG и SHORT
        Возвращает два результата - для LONG и SHORT позиций
        """
        try:
            # ✅ ИСПРАВЛЕНО: Получаем цены входа для LONG и SHORT отдельно
            entry_data_long = self.get_entry_price(
                signal['trading_pair_id'],
                signal['signal_timestamp'],
                'LONG'
            )
            
            entry_data_short = self.get_entry_price(
                signal['trading_pair_id'],
                signal['signal_timestamp'],
                'SHORT'
            )

            # Если нет данных о цене входа - создаем записи с пометкой NO_DATA
            if not entry_data_long or not entry_data_short:
                logger.warning(f"⚠️ Нет цены входа для {signal['pair_symbol']} @ {signal['signal_timestamp']}")
                self.skipped_count += 1
                # ВАЖНО: Возвращаем записи с пометкой no_entry_price, чтобы не зацикливаться
                long_result = self.create_no_data_result(signal, 'LONG', 'no_entry_price')
                short_result = self.create_no_data_result(signal, 'SHORT', 'no_entry_price')
                return long_result, short_result

            # ✅ ИСПРАВЛЕНО: Берем время и цены входа (теперь разные для LONG и SHORT)
            actual_entry_time_long = entry_data_long['entry_time']
            entry_price_long = entry_data_long['entry_price']
            
            actual_entry_time_short = entry_data_short['entry_time']
            entry_price_short = entry_data_short['entry_price']

            # Получаем историю цен за заданный период (5-минутные свечи)
            # ⚠️ ВРЕМЕННО: Используем fas_v2.market_data_aggregated вместо fas_v2
            # TODO: Заменить на fas_v2.market_data_aggregated после миграции данных
            # Используем время от LONG (они должны быть одинаковыми)
            # ✅ ИСПРАВЛЕНО: Убрана SQL injection уязвимость через f-string
            history_query = """
                SELECT
                    timestamp,
                    close_price,
                    high_price,
                    low_price
                FROM fas_v2.market_data_aggregated
                WHERE trading_pair_id = %s
                    AND timeframe = '5m'
                    AND timestamp >= %s
                    AND timestamp <= %s + INTERVAL '1 hour' * %s
                ORDER BY timestamp ASC
            """

            with self.conn.cursor() as cur:
                cur.execute(history_query, (
                    signal['trading_pair_id'],
                    actual_entry_time_long,
                    actual_entry_time_long,
                    self.config.analysis_hours
                ))
                history = cur.fetchall()

            # Динамический расчет: для 5-минутных свечей - 12 свечей в час, допустима потеря не более 25%
            min_candles = int(self.config.analysis_hours * 12 * 0.75)
            if not history or len(history) < min_candles:
                logger.warning(f"⚠️ Недостаточно истории для {signal['pair_symbol']}")
                self.skipped_count += 1
                # ВАЖНО: Возвращаем записи с пометкой insufficient_history, чтобы не зацикливаться
                long_result = self.create_no_data_result(signal, 'LONG', 'insufficient_history')
                short_result = self.create_no_data_result(signal, 'SHORT', 'insufficient_history')
                return long_result, short_result

            # ✅ ИСПРАВЛЕНО: Рассчитываем результаты для обоих направлений с разными ценами входа
            # ✅ ИСПРАВЛЕНО: Передаем scoring_history_id для детерминированного random seed
            long_result = self.calculate_trade_result(
                'LONG',
                entry_price_long,
                history,
                actual_entry_time_long,
                signal['scoring_history_id']
            )

            short_result = self.calculate_trade_result(
                'SHORT',
                entry_price_short,
                history,
                actual_entry_time_short,
                signal['scoring_history_id']
            )

            # Формируем результаты для сохранения
            base_data = {
                'scoring_history_id': signal['scoring_history_id'],
                'signal_timestamp': signal['signal_timestamp'],
                'pair_symbol': signal['pair_symbol'],
                'trading_pair_id': signal['trading_pair_id'],
                'market_regime': signal['market_regime'],
                'total_score': float(signal['total_score']),
                'indicator_score': float(signal['indicator_score']),
                'pattern_score': float(signal['pattern_score']),
                'combination_score': float(signal.get('combination_score', 0)),
                'tp_percent': self.config.tp_percent,
                'sl_percent': self.config.sl_percent,
                'position_size': self.config.position_size,
                'leverage': self.config.leverage,
                'analysis_end_time': actual_entry_time_long + timedelta(hours=self.config.analysis_hours)
            }

            # Результат для LONG
            long_data = {**base_data}
            long_data.update({
                'signal_type': 'LONG',
                'entry_price': long_result.entry_price,
                'best_price': long_result.best_price,
                'worst_price': long_result.worst_price,
                'close_price': long_result.close_price,
                'is_closed': long_result.is_closed,
                'close_reason': long_result.close_reason,
                'is_win': long_result.is_win,
                'close_time': long_result.close_time,
                'hours_to_close': long_result.hours_to_close,
                'pnl_percent': long_result.pnl_percent,
                'pnl_usd': long_result.pnl_usd,
                'max_potential_profit_percent': long_result.max_potential_profit_percent,
                'max_potential_profit_usd': long_result.max_potential_profit_usd,
                'max_drawdown_percent': long_result.max_drawdown_percent,
                'max_drawdown_usd': long_result.max_drawdown_usd
            })

            # Результат для SHORT
            short_data = {**base_data}
            short_data.update({
                'signal_type': 'SHORT',
                'entry_price': short_result.entry_price,
                'best_price': short_result.best_price,
                'worst_price': short_result.worst_price,
                'close_price': short_result.close_price,
                'is_closed': short_result.is_closed,
                'close_reason': short_result.close_reason,
                'is_win': short_result.is_win,
                'close_time': short_result.close_time,
                'hours_to_close': short_result.hours_to_close,
                'pnl_percent': short_result.pnl_percent,
                'pnl_usd': short_result.pnl_usd,
                'max_potential_profit_percent': short_result.max_potential_profit_percent,
                'max_potential_profit_usd': short_result.max_potential_profit_usd,
                'max_drawdown_percent': short_result.max_drawdown_percent,
                'max_drawdown_usd': short_result.max_drawdown_usd
            })

            return long_data, short_data

        except KeyboardInterrupt:
            # Пробросить прерывание пользователем
            raise
        
        except (psycopg.Error, psycopg.OperationalError) as e:
            # Ошибки БД - логируем и продолжаем
            logger.error(f"❌ Ошибка БД при анализе {signal['pair_symbol']}: {e}")
            self.error_count += 1
            return None, None
        
        except (KeyError, ValueError, TypeError) as e:
            # Ошибки данных - логируем с контекстом
            logger.error(f"❌ Ошибка данных в сигнале {signal['pair_symbol']}: {e}")
            logger.debug(f"Проблемный сигнал: {signal}")
            self.error_count += 1
            return None, None
        
        except Exception as e:
            # Неожиданная ошибка - полный traceback для отладки
            logger.exception(f"❌ НЕОЖИДАННАЯ ОШИБКА в сигнале {signal['pair_symbol']}")
            raise  # Пробросить для остановки и отладки

    def save_results(self, results: List[Dict]):
        """
        Сохранение результатов в таблицу с поддержкой обоих направлений
        ✅ ИСПРАВЛЕНО: Правильная обработка транзакций - commit после каждой записи
        """
        if not results:
            return

        insert_query = """
            INSERT INTO web.scoring_history_results_v2 (
                scoring_history_id, signal_timestamp, pair_symbol, trading_pair_id,
                market_regime, total_score, indicator_score, pattern_score, combination_score,
                signal_type, entry_price, best_price, worst_price, close_price,
                is_closed, close_reason, is_win, close_time, hours_to_close,
                pnl_percent, pnl_usd,
                max_potential_profit_percent, max_potential_profit_usd,
                max_drawdown_percent, max_drawdown_usd,
                tp_percent, sl_percent, position_size, leverage,
                analysis_end_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (scoring_history_id, signal_type) DO UPDATE SET
                pnl_percent = EXCLUDED.pnl_percent,
                pnl_usd = EXCLUDED.pnl_usd,
                processed_at = NOW()
        """

        saved_count = 0

        for result in results:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(insert_query, (
                        result['scoring_history_id'],
                        result['signal_timestamp'],
                        result['pair_symbol'],
                        result['trading_pair_id'],
                        result['market_regime'],
                        result['total_score'],
                        result['indicator_score'],
                        result['pattern_score'],
                        result['combination_score'],
                        result['signal_type'],
                        result['entry_price'],
                        result['best_price'],
                        result['worst_price'],
                        result['close_price'],
                        result['is_closed'],
                        result['close_reason'],
                        result.get('is_win'),
                        result.get('close_time'),
                        result.get('hours_to_close'),
                        result['pnl_percent'],
                        result['pnl_usd'],
                        result['max_potential_profit_percent'],
                        result['max_potential_profit_usd'],
                        result['max_drawdown_percent'],
                        result['max_drawdown_usd'],
                        result['tp_percent'],
                        result['sl_percent'],
                        result['position_size'],
                        result['leverage'],
                        result['analysis_end_time']
                    ))
                self.conn.commit()  # ✅ ИСПРАВЛЕНО: Commit после каждой записи
                saved_count += 1
            except Exception as e:
                self.conn.rollback()  # ✅ ДОБАВЛЕНО: Явный rollback при ошибке
                logger.error(f"❌ Ошибка сохранения результата: {e}")
                self.error_count += 1

        self.new_signals_count += saved_count
        logger.info(f"💾 Сохранено {saved_count} результатов из {len(results)}")

    def print_comparative_statistics(self):
        """Вывод сравнительной статистики по LONG и SHORT позициям"""
        try:
            stats_query = """
                WITH stats AS (
                    SELECT
                        signal_type,
                        COUNT(*) as total_signals,
                        COUNT(CASE WHEN is_win = true THEN 1 END) as wins,
                        COUNT(CASE WHEN is_win = false THEN 1 END) as losses,
                        COUNT(CASE WHEN is_win IS NULL AND close_reason = 'timeout' THEN 1 END) as timeouts,
                        AVG(pnl_percent) as avg_pnl_pct,
                        SUM(pnl_usd) as total_pnl,
                        AVG(CASE WHEN is_win = true THEN pnl_usd END) as avg_win_profit,
                        AVG(CASE WHEN is_win = false THEN pnl_usd END) as avg_loss,
                        MAX(pnl_usd) as max_profit,
                        MIN(pnl_usd) as max_loss,
                        AVG(max_potential_profit_percent) as avg_max_potential_pct,
                        AVG(max_drawdown_percent) as avg_max_drawdown_pct,
                        AVG(hours_to_close) FILTER (WHERE close_reason != 'timeout') as avg_hours_to_close
                    FROM web.scoring_history_results_v2
                    WHERE processed_at >= NOW() - INTERVAL '1 day'
                        AND close_reason NOT IN ('insufficient_history', 'no_entry_price')
                    GROUP BY signal_type
                ),
                combined AS (
                    SELECT
                        'COMBINED' as signal_type,
                        COUNT(*) as total_signals,
                        COUNT(CASE WHEN is_win = true THEN 1 END) as wins,
                        COUNT(CASE WHEN is_win = false THEN 1 END) as losses,
                        COUNT(CASE WHEN is_win IS NULL AND close_reason = 'timeout' THEN 1 END) as timeouts,
                        AVG(pnl_percent) as avg_pnl_pct,
                        SUM(pnl_usd) as total_pnl,
                        AVG(CASE WHEN is_win = true THEN pnl_usd END) as avg_win_profit,
                        AVG(CASE WHEN is_win = false THEN pnl_usd END) as avg_loss,
                        MAX(pnl_usd) as max_profit,
                        MIN(pnl_usd) as max_loss,
                        AVG(max_potential_profit_percent) as avg_max_potential_pct,
                        AVG(max_drawdown_percent) as avg_max_drawdown_pct,
                        AVG(hours_to_close) FILTER (WHERE close_reason != 'timeout') as avg_hours_to_close
                    FROM web.scoring_history_results_v2
                    WHERE processed_at >= NOW() - INTERVAL '1 day'
                        AND close_reason NOT IN ('insufficient_history', 'no_entry_price')
                )
                SELECT * FROM stats
                UNION ALL
                SELECT * FROM combined
                ORDER BY signal_type
            """

            with self.conn.cursor() as cur:
                cur.execute(stats_query)
                stats = cur.fetchall()

            logger.info("=" * 80)
            logger.info("📊 СРАВНИТЕЛЬНАЯ СТАТИСТИКА LONG vs SHORT (последние 24 часа):")
            logger.info("=" * 80)

            for stat in stats:
                if stat and stat['total_signals'] > 0:
                    signal_type = stat['signal_type']

                    if signal_type == 'COMBINED':
                        logger.info("\n" + "─" * 40)
                        logger.info("📈 ОБЩАЯ СТАТИСТИКА (LONG + SHORT):")
                    else:
                        logger.info(f"\n📊 {signal_type} ПОЗИЦИИ:")

                    logger.info(f"   ├─ Всего сигналов: {stat['total_signals']}")
                    logger.info(f"   ├─ Победы (TP): {stat['wins']}")
                    logger.info(f"   ├─ Поражения (SL): {stat['losses']}")
                    logger.info(f"   └─ Таймауты: {stat['timeouts']}")

                    if stat['wins'] and stat['losses']:
                        win_rate = stat['wins'] / (stat['wins'] + stat['losses']) * 100
                        logger.info(f"\n🏆 Win Rate: {win_rate:.1f}%")

                    if stat['avg_pnl_pct'] is not None:
                        logger.info(f"\n💰 Финансовые результаты:")
                        logger.info(f"   ├─ Средний P&L: {stat['avg_pnl_pct']:.2f}%")
                        logger.info(f"   ├─ Общий P&L: ${stat['total_pnl']:.2f}" if stat[
                            'total_pnl'] else "   ├─ Общий P&L: $0.00")
                        logger.info(f"   ├─ Средний профит: ${stat['avg_win_profit']:.2f}" if stat[
                            'avg_win_profit'] else "   ├─ Средний профит: N/A")
                        logger.info(f"   └─ Средний убыток: ${stat['avg_loss']:.2f}" if stat[
                            'avg_loss'] else "   └─ Средний убыток: N/A")

                    if stat['avg_max_potential_pct']:
                        logger.info(f"\n🚀 Средний максимальный потенциал: {stat['avg_max_potential_pct']:.2f}%")
                    
                    if stat['avg_max_drawdown_pct']:
                        logger.info(f"⚠️  Средняя максимальная просадка: {stat['avg_max_drawdown_pct']:.2f}%")

                    if stat['avg_hours_to_close']:
                        logger.info(f"⏱️  Среднее время до закрытия: {stat['avg_hours_to_close']:.1f} часов")

            # Дополнительная статистика - какое направление лучше
            logger.info("\n" + "=" * 80)
            logger.info("🎯 РЕКОМЕНДАЦИИ ПО НАПРАВЛЕНИЯМ:")
            logger.info("=" * 80)

            comparison_query = """
                WITH direction_stats AS (
                    SELECT
                        signal_type,
                        COUNT(CASE WHEN is_win = true THEN 1 END)::FLOAT /
                            NULLIF(COUNT(CASE WHEN is_win IS NOT NULL THEN 1 END), 0) as win_rate,
                        AVG(pnl_percent) as avg_pnl_pct,
                        SUM(pnl_usd) as total_pnl
                    FROM web.scoring_history_results_v2
                    WHERE processed_at >= NOW() - INTERVAL '1 day'
                        AND close_reason NOT IN ('insufficient_history', 'no_entry_price')
                    GROUP BY signal_type
                )
                SELECT * FROM direction_stats
            """

            with self.conn.cursor() as cur:
                cur.execute(comparison_query)
                comparisons = cur.fetchall()

            best_winrate = None
            best_pnl = None

            for comp in comparisons:
                if comp['win_rate']:
                    if not best_winrate or comp['win_rate'] > best_winrate['win_rate']:
                        best_winrate = comp
                if comp['avg_pnl_pct']:
                    if not best_pnl or comp['avg_pnl_pct'] > best_pnl['avg_pnl_pct']:
                        best_pnl = comp

            if best_winrate:
                logger.info(f"✅ Лучший Win Rate: {best_winrate['signal_type']} ({best_winrate['win_rate'] * 100:.1f}%)")
            if best_pnl:
                logger.info(f"💰 Лучший средний P&L: {best_pnl['signal_type']} ({best_pnl['avg_pnl_pct']:.2f}%)")

            # Статистика исключенных записей
            logger.info("\n" + "─" * 40)
            logger.info("🗑️  ИСКЛЮЧЕННЫЕ ЗАПИСИ:")
            logger.info("─" * 40)

            excluded_query = """
                SELECT
                    close_reason,
                    COUNT(*) as count
                FROM web.scoring_history_results_v2
                WHERE processed_at >= NOW() - INTERVAL '1 day'
                    AND close_reason IN ('insufficient_history', 'no_entry_price')
                GROUP BY close_reason
                ORDER BY count DESC
            """

            with self.conn.cursor() as cur:
                cur.execute(excluded_query)
                excluded_stats = cur.fetchall()

            total_excluded = 0
            if excluded_stats:
                for exc in excluded_stats:
                    count = exc['count']
                    reason = exc['close_reason']
                    total_excluded += count

                    if reason == 'insufficient_history':
                        logger.info(f"   ├─ Недостаточно свечей: {count}")
                    elif reason == 'no_entry_price':
                        logger.info(f"   ├─ Нет цены входа: {count}")
                    else:
                        logger.info(f"   ├─ {reason}: {count}")

                logger.info(f"   └─ Всего исключено: {total_excluded}")
            else:
                logger.info("   └─ Нет исключенных записей")

            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"❌ Ошибка при выводе статистики: {e}")

    def run(self):
        """Основной процесс анализа"""
        start_time = datetime.now()

        logger.info("🚀 Начало анализа исторических данных скоринга (v7.0 - UNIFIED PERIODS)")
        logger.info(f"📅 Время запуска: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info(f"✨ Окно анализа: {self.config.analysis_hours} часов с 5-минутными свечами")
        logger.info("✨ Расчет результатов одновременно для LONG и SHORT позиций")
        logger.info("✅ УНИФИЦИРОВАННЫЕ ПЕРИОДЫ: согласованы с SQL функциями")
        logger.info("✅ ДЕТЕРМИНИРОВАННЫЙ SEED: каждый сигнал имеет уникальный seed")
        logger.warning("⚠️  ВРЕМЕННО: Используется fas_v2.market_data_aggregated (не fas_v2)")
        logger.warning("⚠️  ПРИЧИНА: В fas_v2 отсутствуют данные для 520 торговых пар")

        try:
            self.connect()

            # Получаем и логируем период анализа
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM fas_v2.get_analysis_period('monthly')")
                period_info = cur.fetchone()

            logger.info(f"📊 Период анализа: {period_info['period_start'].date()} to {period_info['period_end'].date()}")
            logger.info(f"📊 Количество дней: {period_info['days_count']}")

            batch_number = 0
            total_processed_in_run = 0

            while True:
                batch_number += 1

                signals = self.get_unprocessed_signals(self.config.batch_size)

                if not signals:
                    if batch_number == 1:
                        logger.info("✅ Нет новых сигналов для обработки")
                    else:
                        logger.info("✅ Все сигналы обработаны!")
                    break

                logger.info(f"\n📦 Обработка пакета #{batch_number}")
                logger.info(f"📊 В пакете: {len(signals)} сигналов")

                results = []
                batch_processed = 0

                for i, signal in enumerate(signals):
                    if i % 100 == 0 and i > 0:
                        progress = (i / len(signals)) * 100
                        logger.info(f"⏳ Пакет #{batch_number}: {i}/{len(signals)} ({progress:.1f}%)")

                    # Анализируем сигнал для обоих направлений
                    long_result, short_result = self.analyze_signal_both_directions(signal)

                    # Сохраняем результаты даже если это записи NO_DATA
                    if long_result and short_result:
                        results.append(long_result)
                        results.append(short_result)
                        self.processed_count += 1
                        batch_processed += 1

                    # Сохраняем результаты батчами
                    if len(results) >= self.config.save_batch_size:
                        self.save_results(results)
                        results = []

                # Сохраняем оставшиеся результаты
                if results:
                    self.save_results(results)

                total_processed_in_run += batch_processed
                logger.info(f"✅ Пакет #{batch_number} обработан: {batch_processed} сигналов")

                # Небольшая пауза между пакетами
                if batch_processed > 0:
                    time.sleep(1)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info("\n" + "=" * 80)
            logger.info("📋 ИТОГИ ОБРАБОТКИ:")
            logger.info("=" * 80)
            logger.info(f"✅ Успешно обработано сигналов: {self.processed_count}")
            logger.info(f"💾 Сохранено результатов: {self.new_signals_count} (по 2 на каждый сигнал)")
            logger.info(f"⭕ Пропущено (нет данных): {self.skipped_count}")
            logger.info(f"❌ Ошибок: {self.error_count}")
            logger.info(f"⏱️  Время выполнения: {duration:.1f} секунд ({duration / 60:.1f} минут)")

            if self.processed_count > 0 and duration > 0:
                logger.info(f"⚡ Скорость обработки: {self.processed_count / duration:.1f} сигналов/сек")

            logger.info("=" * 80)

            # Выводим сравнительную статистику
            self.print_comparative_statistics()

        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Точка входа"""
    load_dotenv()
    try:
        analyzer = ImprovedScoringAnalyzer()
        analyzer.run()
    except KeyboardInterrupt:
        logger.info("\n⛔ Прерывание пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Фатальная ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
