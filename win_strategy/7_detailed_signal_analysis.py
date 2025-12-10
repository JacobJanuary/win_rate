#!/usr/bin/env python3
"""
Step 7: Detailed Signal Analysis
Shows detailed simulation results for each signal

Usage:
    python3 7_detailed_signal_analysis.py
    python3 7_detailed_signal_analysis.py --combination-id 1
    python3 7_detailed_signal_analysis.py --limit 20
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import argparse
from datetime import datetime, timedelta
from optimization.utils.db_helper import DatabaseHelper
from optimization.utils.simulation_engine import SimulationEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_combinations_with_params(db: DatabaseHelper, combination_id: int = None):
    """Load combinations with their best parameters"""
    
    where_clause = f"WHERE wc.id = {combination_id}" if combination_id else "WHERE wc.is_active = true"
    
    query = f"""
        SELECT 
            wc.id,
            wc.combination_name,
            wc.patterns,
            wc.signal_type,
            wc.market_regime,
            cbp.sl_pct,
            cbp.ts_activation_pct,
            cbp.ts_callback_pct,
            cbp.win_rate,
            cbp.total_pnl_pct
        FROM optimization.winning_combinations wc
        JOIN optimization.combination_best_parameters cbp ON cbp.combination_id = wc.id
        {where_clause}
        ORDER BY cbp.total_pnl_pct DESC
    """
    
    return db.execute_query(query)


def load_signals_for_combination(db: DatabaseHelper, combination_id: int, limit: int = None):
    """Load signals for a specific combination"""
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
        SELECT 
            cs.id,
            cs.pair_symbol,
            cs.signal_timestamp,
            cs.entry_time,
            cs.signal_type
        FROM optimization.combination_signals cs
        WHERE cs.combination_id = %s
        ORDER BY cs.signal_timestamp DESC
        {limit_clause}
    """
    
    return db.execute_query(query, (combination_id,))


def load_candles_for_signal(db: DatabaseHelper, signal_id: int):
    """Load candles for a signal"""
    
    query = """
        SELECT 
            open_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM optimization.combination_candles
        WHERE signal_id = %s
        ORDER BY open_time
    """
    
    return db.execute_query(query, (signal_id,))


def simulate_signal_detailed(signal, candles, params):
    """
    Simulate signal with detailed tracking
    
    Returns detailed simulation results with exit analysis
    """
    
    if not candles or len(candles) < 100:
        return None
    
    engine = SimulationEngine()
    
    # Run simulation
    result = engine.simulate_trade(
        candles=candles,
        signal_type=signal['signal_type'],
        sl_pct=float(params['sl_pct']),
        ts_activation_pct=float(params['ts_activation_pct']),
        ts_callback_pct=float(params['ts_callback_pct'])
    )
    
    if not result:
        return None
    
    # Calculate additional metrics
    entry_price = candles[0]['close_price']
    is_long = signal['signal_type'] == 'LONG'
    
    max_pump_pct = 0
    max_pump_time = 0
    max_dd_pct = 0
    max_dd_time = 0
    
    for idx, candle in enumerate(candles[:result['hold_duration_minutes']]):
        # Calculate current profit
        if is_long:
            profit_pct = ((candle['high_price'] - entry_price) / entry_price) * 100
            loss_pct = ((candle['low_price'] - entry_price) / entry_price) * 100
        else:
            profit_pct = ((entry_price - candle['low_price']) / entry_price) * 100
            loss_pct = ((entry_price - candle['high_price']) / entry_price) * 100
        
        # Track max pump
        if profit_pct > max_pump_pct:
            max_pump_pct = profit_pct
            max_pump_time = idx
        
        # Track max drawdown
        if loss_pct < max_dd_pct:
            max_dd_pct = loss_pct
            max_dd_time = idx
    
    # Build details string
    details = []
    
    if result['exit_type'].startswith('TS'):
        # Find TS activation time
        ts_activation_time = None
        for idx, candle in enumerate(candles):
            if is_long:
                profit = ((candle['high_price'] - entry_price) / entry_price) * 100
            else:
                profit = ((entry_price - candle['low_price']) / entry_price) * 100
            
            if profit >= float(params['ts_activation_pct']):
                ts_activation_time = idx
                break
        
        if ts_activation_time is not None:
            details.append(f"TS activated @{ts_activation_time:02d}:{ts_activation_time%60:02d}")
        details.append(f"peak {max_pump_pct:.2f}%")
    
    elif result['exit_type'] == 'SL':
        details.append("Stop loss hit")
    
    elif result['exit_type'].startswith('TIMEOUT'):
        details.append(f"Timeout at {result['hold_duration_minutes']//60}h")
        if max_pump_pct >= float(params['ts_activation_pct']):
            details.append("TS not activated")
    
    return {
        'signal_time': signal['signal_timestamp'],
        'symbol': signal['pair_symbol'],
        'exit_type': result['exit_type'],
        'exit_time_minutes': result['hold_duration_minutes'],
        'pnl_pct': result['pnl_pct'],
        'max_drawdown_pct': max_dd_pct,
        'max_drawdown_time': max_dd_time,
        'max_pump_pct': max_pump_pct,
        'max_pump_time': max_pump_time,
        'details': ', '.join(details) if details else '-'
    }


def format_time(minutes):
    """Format minutes to HH:MM"""
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def print_results(combination, signals_results):
    """Print detailed results table"""
    
    print("\n" + "="*160)
    print(f"COMBINATION: {combination['combination_name']}")
    print(f"Parameters: SL={combination['sl_pct']}%, TS_ACT={combination['ts_activation_pct']}%, TS_CB={combination['ts_callback_pct']}%")
    print("="*160)
    
    # Header
    print(f"\n{'Signal Time':<20} {'Symbol':<12} {'Exit':<15} {'Time':<8} {'PnL':<9} {'Max DD':<17} {'Max Pump':<17} {'Details':<50}")
    print("-"*160)
    
    # Signals
    for result in signals_results:
        signal_time = result['signal_time'].strftime('%Y-%m-%d %H:%M')
        exit_time = format_time(result['exit_time_minutes'])
        pnl_str = f"{result['pnl_pct']:>6.2f}%"
        
        # Max DD with time
        dd_time = format_time(result['max_drawdown_time'])
        dd_str = f"{result['max_drawdown_pct']:>6.2f}% @{dd_time}"
        
        # Max Pump with time
        pump_time = format_time(result['max_pump_time'])
        pump_str = f"{result['max_pump_pct']:>6.2f}% @{pump_time}"
        
        print(f"{signal_time:<20} {result['symbol']:<12} {result['exit_type']:<15} {exit_time:<8} {pnl_str:<9} {dd_str:<17} {pump_str:<17} {result['details']:<50}")
    
    # Summary
    print("\n" + "-"*160)
    print("SUMMARY:")
    
    total = len(signals_results)
    avg_pnl = sum(r['pnl_pct'] for r in signals_results) / total if total > 0 else 0
    wins = sum(1 for r in signals_results if r['pnl_pct'] > 0)
    win_rate = (wins / total * 100) if total > 0 else 0
    
    # Exit type breakdown
    exit_types = {}
    for r in signals_results:
        exit_types[r['exit_type']] = exit_types.get(r['exit_type'], 0) + 1
    
    print(f"  Total Signals: {total}")
    print(f"  Avg PnL: {avg_pnl:.2f}%")
    print(f"  Win Rate: {win_rate:.1f}%")
    print(f"  Exits: {', '.join(f'{k}={v}' for k, v in sorted(exit_types.items()))}")
    print()


def main():
    parser = argparse.ArgumentParser(description='Detailed signal analysis')
    parser.add_argument('--combination-id', type=int, default=None,
                       help='Specific combination ID (default: all)')
    parser.add_argument('--limit', type=int, default=50,
                       help='Max signals per combination (default: 50)')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("DETAILED SIGNAL ANALYSIS")
    logger.info("="*120)
    logger.info(f"Combination: {args.combination_id or 'ALL'}")
    logger.info(f"Limit: {args.limit} signals per combination\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load combinations
    logger.info("Loading combinations...")
    combinations = load_combinations_with_params(db, args.combination_id)
    logger.info(f"Found {len(combinations)} combinations\n")
    
    if not combinations:
        logger.warning("No combinations found")
        db.close()
        return
    
    # Process each combination
    for idx, combo in enumerate(combinations, 1):
        logger.info(f"[{idx}/{len(combinations)}] Processing: {combo['combination_name']}")
        
        # Load signals
        signals = load_signals_for_combination(db, combo['id'], args.limit)
        logger.info(f"  Found {len(signals)} signals")
        
        if not signals:
            continue
        
        # Simulate each signal
        results = []
        for sig_idx, signal in enumerate(signals, 1):
            logger.info(f"  [{sig_idx}/{len(signals)}] Simulating {signal['pair_symbol']}...")
            
            candles = load_candles_for_signal(db, signal['id'])
            if not candles:
                logger.warning(f"    No candles found")
                continue
            
            result = simulate_signal_detailed(signal, candles, combo)
            if result:
                results.append(result)
        
        # Print results
        if results:
            print_results(combo, results)
    
    # Close connection
    db.close()
    logger.info("\n✅ Analysis complete!")


if __name__ == "__main__":
    main()
