#!/usr/bin/env python3
"""
Run Trade Simulations with All Parameter Combinations
Step 3 of optimization pipeline
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import yaml
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


def load_config():
    """Load optimization config"""
    with open('config/optimization_config.yaml', 'r') as f:
        return yaml.safe_load(f)


def get_parameter_combinations(config):
    """Generate all parameter combinations"""
    sl_range = config['simulation']['sl_range']
    ts_act_range = config['simulation']['ts_activation_range']
    ts_cb_range = config['simulation']['ts_callback_range']
    
    combos = list(product(sl_range, ts_act_range, ts_cb_range))
    
    logger.info(f"Parameter space:")
    logger.info(f"  SL: {sl_range}")
    logger.info(f"  TS Activation: {ts_act_range}")
    logger.info(f"  TS Callback: {ts_cb_range}")
    logger.info(f"  Total combinations: {len(combos)}")
    
    return combos


def load_signals(db: DatabaseHelper):
    """Load all selected signals"""
    query = """
        SELECT 
            id,
            signal_id,
            pair_symbol,
            entry_time,
            signal_type,
            patterns,
            market_regime,
            strategy_name
        FROM optimization.selected_signals
        ORDER BY id
    """
    
    return db.execute_query(query)


def load_candles_for_signal(db: DatabaseHelper, signal):
    """Load 1m candles for a signal"""
    entry_time_ms = int(signal['entry_time'].timestamp() * 1000)
    end_time_ms = entry_time_ms + (24 * 60 * 60 * 1000)
    
    query = """
        SELECT 
            open_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM optimization.candles_1m
        WHERE pair_symbol = %s
            AND open_time >= %s
            AND open_time < %s
        ORDER BY open_time
    """
    
    return db.execute_query(query, (signal['pair_symbol'], entry_time_ms, end_time_ms))


def simulate_signal(args):
    """Simulate all param combinations for a signal (worker function)"""
    signal, param_combos = args
    
    # Create new DB connection for this worker
    db = DatabaseHelper()
    db.connect()
    
    # Load candles
    candles = load_candles_for_signal(db, signal)
    
    if len(candles) < 100:  # Need sufficient candles
        logger.warning(f"Insufficient candles for {signal['pair_symbol']}: {len(candles)}")
        db.close()
        return 0
    
    # Run simulations
    engine = SimulationEngine()
    results = engine.simulate_batch(signal, candles, param_combos)
    
    # Insert results in controlled batches
    insert_data = []
    for result in results:
        insert_data.append((
            result['signal_id'],
            result['sl_pct'],
            result['ts_activation_pct'],
            result['ts_callback_pct'],
            result['exit_type'],
            result['exit_price'],
            result['exit_time'],
            result['pnl_pct'],
            result['max_profit_pct'],
            result['max_drawdown_pct'],
            result['hold_duration_minutes']
        ))
    
    try:
        # Bulk insert all simulations for this signal
        if insert_data:
            try:
                db.bulk_insert_simulations(
                    'optimization.simulation_results',
                    ['signal_id', 'sl_pct', 'ts_activation_pct', 'ts_callback_pct',
                     'exit_type', 'exit_price', 'exit_time', 'pnl_pct',
                     'max_profit_pct', 'max_drawdown_pct', 'hold_duration_minutes'],
                    insert_data
                )
            except Exception as e:
                logger.error(f"Error inserting results for signal {signal['signal_id']}: {e}")
                db.close()
                # Re-establish connection for subsequent operations if needed, though this worker is about to finish
                db.connect() 
                return 0 # Indicate failure for this signal
    except Exception as e:
        logger.error(f"Error preparing or inserting results for signal {signal['signal_id']}: {e}")
        db.close()
        return 0
    
    db.close()
    
    return len(results)


def main():
    logger.info("=" * 100)
    logger.info("TRADE SIMULATION RUNNER")
    logger.info("=" * 100)
    logger.info("")
    
    # Load config
    config = load_config()
    
    # Get parameter combinations
    logger.info("Step 1: Generating parameter combinations...")
    param_combos = get_parameter_combinations(config)
    
    # Connect to database
    logger.info("\nStep 2: Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Load signals
    logger.info("\nStep 3: Loading signals...")
    signals = load_signals(db)
    logger.info(f"Found {len(signals)} signals to simulate")
    
    # Calculate total simulations
    total_sims = len(signals) * len(param_combos)
    logger.info(f"\nTotal simulations to run: {total_sims:,}")
    logger.info(f"Estimated time: {total_sims / 10000:.1f} - {total_sims / 5000:.1f} minutes")
    
    db.close()
    
    # Run simulations in parallel
    logger.info(f"\nStep 4: Running simulations...")
    workers = min(config['simulation']['parallel_workers'], cpu_count())
    logger.info(f"Using {workers} parallel workers\n")
    
    # Prepare args for workers
    worker_args = [(signal, param_combos) for signal in signals]
    
    # Run with multiprocessing
    total_results = 0
    
    with Pool(processes=workers) as pool:
        for count in tqdm(
            pool.imap_unordered(simulate_signal, worker_args),
            total=len(signals),
            desc="Simulating signals"
        ):
            total_results += count
    
    logger.info("\n" + "=" * 100)
    logger.info("SIMULATION COMPLETE!")
    logger.info("=" * 100)
    logger.info(f"✅ Total simulations completed: {total_results:,}")
    logger.info(f"✅ Signals processed: {len(signals)}")
    logger.info(f"✅ Average sims per signal: {total_results/len(signals):.0f}")
    logger.info("=" * 100)


if __name__ == "__main__":
    main()
