#!/usr/bin/env python3
"""
Step 3: Simulate Yesterday's Trades
Run trade simulation using optimized parameters for each signal
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from optimization.utils.db_helper import DatabaseHelper
from optimization.utils.trade_simulator import TradeSimulator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_signals_to_simulate(db: DatabaseHelper):
    """Load signals that haven't been simulated yet"""
    
    query = """
        SELECT 
            ys.signal_id,
            ys.pair_symbol,
            ys.entry_time,
            ys.signal_type,
            ys.sl_pct,
            ys.ts_activation_pct,
            ys.ts_callback_pct
        FROM optimization.yesterday_signals ys
        WHERE NOT EXISTS (
            SELECT 1 
            FROM optimization.yesterday_results yr
            WHERE yr.signal_id = ys.signal_id
        )
        ORDER BY ys.entry_time
    """
    
    return db.execute_query(query)


def load_candles_for_signal(db: DatabaseHelper, pair_symbol, entry_time):
    """Load 24h of 1m candles for simulation"""
    
    start_time_ms = int(entry_time.timestamp() * 1000)
    end_time_ms = start_time_ms + (24 * 60 * 60 * 1000)
    
    query = """
        SELECT 
            open_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM optimization.yesterday_candles
        WHERE pair_symbol = %(pair)s
            AND open_time >= %(start)s
            AND open_time < %(end)s
        ORDER BY open_time
    """
    
    params = {
        'pair': pair_symbol,
        'start': start_time_ms,
        'end': end_time_ms
    }
    
    return db.execute_query(query, params)


def simulate_signal(signal, candles):
    """Simulate trade with given parameters"""
    
    if not candles or len(candles) < 10:
        logger.warning(f"Insufficient candles for {signal['pair_symbol']}")
        return None
    
    # Get entry price (first candle)
    entry_price = float(candles[0]['open_price'])
    
    # Convert parameters
    sl_pct = float(signal['sl_pct'])
    ts_activation_pct = float(signal['ts_activation_pct'])
    ts_callback_pct = float(signal['ts_callback_pct'])
    signal_type = signal['signal_type']
    
    # Calculate levels
    if signal_type == 'LONG':
        sl_price = entry_price * (1 - sl_pct / 100)
        ts_activation_price = entry_price * (1 + ts_activation_pct / 100)
    else:  # SHORT
        sl_price = entry_price * (1 + sl_pct / 100)
        ts_activation_price = entry_price * (1 - ts_activation_pct / 100)
    
    # Simulate
    ts_active = False
    ts_highest = entry_price if signal_type == 'LONG' else entry_price
    max_profit = 0
    max_drawdown = 0
    
    for idx, candle in enumerate(candles):
        high = float(candle['high_price'])
        low = float(candle['low_price'])
        close = float(candle['close_price'])
        
        # Calculate current P&L
        if signal_type == 'LONG':
            current_pnl = (close / entry_price - 1) * 100
            max_profit = max(max_profit, (high / entry_price - 1) * 100)
            max_drawdown = min(max_drawdown, (low / entry_price - 1) * 100)
            
            # Check SL
            if low <= sl_price:
                return {
                    'exit_type': 'SL',
                    'exit_price': sl_price,
                    'exit_time': int(candle['open_time']),
                    'pnl_pct': -sl_pct,
                    'max_profit_pct': max_profit,
                    'max_drawdown_pct': max_drawdown,
                    'hold_duration_minutes': idx
                }
            
            # Check TS activation
            if not ts_active and high >= ts_activation_price:
                ts_active = True
                ts_highest = high
            
            # Update TS
            if ts_active:
                ts_highest = max(ts_highest, high)
                ts_exit_price = ts_highest * (1 - ts_callback_pct / 100)
                
                if low <= ts_exit_price:
                    pnl = (ts_exit_price / entry_price - 1) * 100
                    return {
                        'exit_type': 'TS',
                        'exit_price': ts_exit_price,
                        'exit_time': int(candle['open_time']),
                        'pnl_pct': pnl,
                        'max_profit_pct': max_profit,
                        'max_drawdown_pct': max_drawdown,
                        'hold_duration_minutes': idx
                    }
        
        else:  # SHORT
            current_pnl = (1 - close / entry_price) * 100
            max_profit = max(max_profit, (1 - low / entry_price) * 100)
            max_drawdown = min(max_drawdown, (1 - high / entry_price) * 100)
            
            # Check SL
            if high >= sl_price:
                return {
                    'exit_type': 'SL',
                    'exit_price': sl_price,
                    'exit_time': int(candle['open_time']),
                    'pnl_pct': -sl_pct,
                    'max_profit_pct': max_profit,
                    'max_drawdown_pct': max_drawdown,
                    'hold_duration_minutes': idx
                }
            
            # Check TS activation
            if not ts_active and low <= ts_activation_price:
                ts_active = True
                ts_highest = low
            
            # Update TS
            if ts_active:
                ts_highest = min(ts_highest, low)
                ts_exit_price = ts_highest * (1 + ts_callback_pct / 100)
                
                if high >= ts_exit_price:
                    pnl = (1 - ts_exit_price / entry_price) * 100
                    return {
                        'exit_type': 'TS',
                        'exit_price': ts_exit_price,
                        'exit_time': int(candle['open_time']),
                        'pnl_pct': pnl,
                        'max_profit_pct': max_profit,
                        'max_drawdown_pct': max_drawdown,
                        'hold_duration_minutes': idx
                    }
    
    # Timeout (24 hours)
    final_price = float(candles[-1]['close_price'])
    if signal_type == 'LONG':
        pnl = (final_price / entry_price - 1) * 100
    else:
        pnl = (1 - final_price / entry_price) * 100
    
    return {
        'exit_type': 'TIME_LIMIT',
        'exit_price': final_price,
        'exit_time': int(candles[-1]['open_time']),
        'pnl_pct': pnl,
        'max_profit_pct': max_profit,
        'max_drawdown_pct': max_drawdown,
        'hold_duration_minutes': len(candles)
    }


def save_result(db: DatabaseHelper, signal_id, sl_pct, ts_act, ts_cb, result):
    """Save simulation result"""
    
    query = """
        INSERT INTO optimization.yesterday_results
        (signal_id, sl_pct, ts_activation_pct, ts_callback_pct,
         exit_type, exit_price, exit_time, pnl_pct,
         max_profit_pct, max_drawdown_pct, hold_duration_minutes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (signal_id) DO NOTHING
    """
    
    with db.conn.cursor() as cur:
        cur.execute(query, (
            signal_id,
            sl_pct,
            ts_act,
            ts_cb,
            result['exit_type'],
            result['exit_price'],
            result['exit_time'],
            result['pnl_pct'],
            result['max_profit_pct'],
            result['max_drawdown_pct'],
            result['hold_duration_minutes']
        ))
        db.conn.commit()


def main():
    logger.info("=" * 100)
    logger.info("YESTERDAY TRADE SIMULATION")
    logger.info("=" * 100)
    logger.info("")
    
    # Connect
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Load signals
    logger.info("Loading signals to simulate...")
    signals = load_signals_to_simulate(db)
    
    if not signals:
        logger.info("✅ All signals already simulated!")
        db.close()
        return
    
    logger.info(f"Found {len(signals)} signals to simulate\n")
    
    # Simulate each
    logger.info("Running simulations...")
    success = 0
    failed = 0
    
    for signal in signals:
        try:
            # Load candles
            candles = load_candles_for_signal(db, signal['pair_symbol'], signal['entry_time'])
            
            if not candles:
                logger.warning(f"No candles for {signal['pair_symbol']}")
                failed += 1
                continue
            
            # Simulate
            result = simulate_signal(signal, candles)
            
            if not result:
                failed += 1
                continue
            
            # Save
            save_result(db, signal['signal_id'], signal['sl_pct'], 
                       signal['ts_activation_pct'], signal['ts_callback_pct'], result)
            
            success += 1
            
            if success % 100 == 0:
                logger.info(f"  Processed: {success}/{len(signals)}")
        
        except Exception as e:
            logger.error(f"Error simulating signal {signal['signal_id']}: {e}")
            if db.conn:
                db.conn.rollback()
            failed += 1
    
    # Summary
    logger.info("\n" + "=" * 100)
    logger.info("SUMMARY:")
    logger.info("=" * 100)
    logger.info(f"✅ Successful simulations: {success}")
    logger.info(f"❌ Failed: {failed}")
    logger.info("=" * 100)
    
    db.close()
    logger.info("\n✅ Complete!")


if __name__ == "__main__":
    main()
