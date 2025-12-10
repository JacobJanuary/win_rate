#!/usr/bin/env python3
"""
Step 4: Simulate Trades with Parameter Grid
Runs simulations for all parameter combinations

Usage:
    python3 4_simulate_trades.py
    python3 4_simulate_trades.py --batch-size 50
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import argparse
from itertools import product
from multiprocessing import Pool, cpu_count
from optimization.utils.db_helper import DatabaseHelper
from optimization.utils.simulation_engine import SimulationEngine
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parameter ranges
SL_RANGE = [1, 2, 3, 4, 5, 6]
TS_ACTIVATION_RANGE = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
TS_CALLBACK_RANGE = [0.5, 1.0, 1.5, 2.0]


def load_signals_with_candles(db: DatabaseHelper, limit: int = None):
    """Load signals that have candles but no simulations yet"""
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
        SELECT DISTINCT
            cs.id,
            cs.combination_id,
            cs.pair_symbol,
            cs.signal_type,
            cs.entry_time
        FROM optimization.combination_signals cs
        JOIN optimization.combination_candles cc ON cc.signal_id = cs.id
        LEFT JOIN optimization.combination_simulations csim ON csim.signal_id = cs.id
        WHERE csim.id IS NULL
        GROUP BY cs.id, cs.combination_id, cs.pair_symbol, cs.signal_type, cs.entry_time
        HAVING COUNT(DISTINCT cc.id) >= 1400  -- At least 1400 candles
        ORDER BY cs.entry_time
        {limit_clause}
    """
    
    results = db.execute_query(query)
    logger.info(f"Found {len(results)} signals ready for simulation")
    
    return results


def load_candles_for_signal(db: DatabaseHelper, signal_id: int):
    """Load candles for a specific signal"""
    
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


def simulate_signal(args):
    """
    Simulate a single signal with all parameter combinations
    
    Args:
        args: Tuple of (signal, candles, param_combos)
    
    Returns:
        List of simulation results
    """
    signal, candles, param_combos = args
    
    # Initialize engine
    engine = SimulationEngine()
    
    # Run simulations
    results = []
    for sl, ts_act, ts_cb in param_combos:
        result = engine.simulate_trade(
            candles=candles,
            signal_type=signal['signal_type'],
            sl_pct=sl,
            ts_activation_pct=ts_act,
            ts_callback_pct=ts_cb
        )
        
        if result:
            results.append({
                'signal_id': signal['id'],
                'combination_id': signal['combination_id'],
                'sl_pct': sl,
                'ts_activation_pct': ts_act,
                'ts_callback_pct': ts_cb,
                **result
            })
    
    return results


def save_simulations(db: DatabaseHelper, simulations: list):
    """Save simulation results to database"""
    
    if not simulations:
        return 0
    
    saved_count = 0
    for sim in simulations:
        try:
            query = """
                INSERT INTO optimization.combination_simulations 
                (signal_id, combination_id, sl_pct, ts_activation_pct, ts_callback_pct,
                 exit_type, exit_price, exit_time, pnl_pct, max_profit_pct, 
                 max_drawdown_pct, hold_duration_minutes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (signal_id, sl_pct, ts_activation_pct, ts_callback_pct) DO NOTHING
            """
            
            db.execute_update(query, (
                sim['signal_id'],
                sim['combination_id'],
                sim['sl_pct'],
                sim['ts_activation_pct'],
                sim['ts_callback_pct'],
                sim['exit_type'],
                sim['exit_price'],
                sim['exit_time'],
                sim['pnl_pct'],
                sim['max_profit_pct'],
                sim['max_drawdown_pct'],
                sim['hold_duration_minutes']
            ))
            
            saved_count += 1
            
        except Exception as e:
            logger.error(f"Error saving simulation: {e}")
            continue
    
    return saved_count


def main():
    parser = argparse.ArgumentParser(description='Simulate trades with parameter grid')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of signals to process (default: 100, 0 = all)')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("SIMULATE TRADES WITH PARAMETER GRID")
    logger.info("="*120)
    logger.info(f"Batch size: {args.batch_size if args.batch_size > 0 else 'ALL'}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Parameter combinations: {len(SL_RANGE) * len(TS_ACTIVATION_RANGE) * len(TS_CALLBACK_RANGE)}\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load signals
    logger.info("Step 1: Loading signals with candles...")
    limit = args.batch_size if args.batch_size > 0 else None
    signals = load_signals_with_candles(db, limit)
    
    if not signals:
        logger.info("No signals ready for simulation. Run 3_fetch_candles.py first.")
        db.close()
        return
    
    # Generate parameter combinations
    param_combos = list(product(SL_RANGE, TS_ACTIVATION_RANGE, TS_CALLBACK_RANGE))
    logger.info(f"Parameter combinations: {len(param_combos)}")
    logger.info(f"Total simulations: {len(signals) * len(param_combos):,}\n")
    
    # Prepare simulation tasks
    logger.info("Step 2: Loading candles for signals...")
    tasks = []
    for signal in signals:
        candles = load_candles_for_signal(db, signal['id'])
        if len(candles) >= 1400:
            tasks.append((signal, candles, param_combos))
        else:
            logger.warning(f"Signal {signal['id']} has only {len(candles)} candles, skipping")
    
    logger.info(f"Prepared {len(tasks)} simulation tasks\n")
    
    # Run simulations
    logger.info("Step 3: Running simulations...")
    total_saved = 0
    
    with Pool(processes=args.workers) as pool:
        for results in tqdm(
            pool.imap(simulate_signal, tasks),
            total=len(tasks),
            desc="Simulating signals"
        ):
            if results:
                saved = save_simulations(db, results)
                total_saved += saved
    
    # Summary
    print("\n" + "="*120)
    print("SUMMARY")
    print("="*120)
    print(f"Signals processed: {len(tasks)}")
    print(f"Total simulations saved: {total_saved:,}")
    print(f"Average simulations per signal: {total_saved/len(tasks):.0f}")
    print("="*120)
    
    # Close connection
    db.close()
    logger.info("\n✅ Simulation complete!")


if __name__ == "__main__":
    main()
