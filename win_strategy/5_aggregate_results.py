#!/usr/bin/env python3
"""
Step 5: Aggregate Results and Find Best Parameters
Analyzes simulation results to find optimal parameters for each combination

Usage:
    python3 5_aggregate_results.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import pandas as pd
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_combinations(db: DatabaseHelper):
    """Load all active combinations"""
    
    query = """
        SELECT id, combination_name, win_rate
        FROM optimization.winning_combinations
        WHERE is_active = true
        ORDER BY win_rate DESC
    """
    
    return db.execute_query(query)


def aggregate_for_combination(db: DatabaseHelper, combination_id: int):
    """
    Aggregate simulation results for a combination
    
    Returns:
        DataFrame with metrics for each parameter set
    """
    
    query = """
        SELECT 
            sl_pct,
            ts_activation_pct,
            ts_callback_pct,
            COUNT(*) as total_signals,
            SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as winning_trades,
            SUM(CASE WHEN pnl_pct <= 0 THEN 1 ELSE 0 END) as losing_trades,
            ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as win_rate,
            ROUND(SUM(pnl_pct), 2) as total_pnl_pct,
            ROUND(AVG(pnl_pct), 2) as avg_pnl_pct,
            ROUND(AVG(CASE WHEN pnl_pct > 0 THEN pnl_pct END), 2) as avg_win_pct,
            ROUND(AVG(CASE WHEN pnl_pct <= 0 THEN pnl_pct END), 2) as avg_loss_pct,
            ROUND(MAX(max_profit_pct), 2) as max_profit_pct,
            ROUND(MIN(max_drawdown_pct), 2) as max_drawdown_pct,
            ROUND(AVG(hold_duration_minutes), 2) as avg_hold_minutes
        FROM optimization.combination_simulations
        WHERE combination_id = %s
        GROUP BY sl_pct, ts_activation_pct, ts_callback_pct
        HAVING COUNT(*) >= 5  -- At least 5 signals
    """
    
    results = db.execute_query(query, (combination_id,))
    
    if not results:
        return None
    
    df = pd.DataFrame(results)
    
    # Calculate profit factor
    df['profit_factor'] = df.apply(
        lambda row: abs(row['avg_win_pct'] * row['winning_trades'] / 
                       (row['avg_loss_pct'] * row['losing_trades']))
        if row['losing_trades'] > 0 and row['avg_loss_pct'] != 0 else None,
        axis=1
    )
    
    # Calculate Sharpe ratio (simplified)
    df['sharpe_ratio'] = df.apply(
        lambda row: row['avg_pnl_pct'] / df['avg_pnl_pct'].std()
        if df['avg_pnl_pct'].std() > 0 else 0,
        axis=1
    )
    
    return df


def find_best_parameters(df: pd.DataFrame, min_win_rate: float = 60.0):
    """
    Find best parameters from aggregated results
    
    Criteria:
    1. Win rate >= min_win_rate
    2. Maximum total_pnl_pct
    """
    
    # Filter by min win rate
    filtered = df[df['win_rate'] >= min_win_rate]
    
    if len(filtered) == 0:
        logger.warning(f"No parameters with WR >= {min_win_rate}%, using all")
        filtered = df
    
    # Sort by total PnL
    best = filtered.nlargest(1, 'total_pnl_pct')
    
    if len(best) == 0:
        return None
    
    return best.iloc[0].to_dict()


def save_best_parameters(db: DatabaseHelper, combination_id: int, params: dict, days: int):
    """Save best parameters to database"""
    
    query = """
        INSERT INTO optimization.combination_best_parameters 
        (combination_id, sl_pct, ts_activation_pct, ts_callback_pct,
         total_signals, winning_trades, losing_trades, win_rate,
         total_pnl_pct, avg_pnl_pct, avg_win_pct, avg_loss_pct,
         max_profit_pct, max_drawdown_pct, avg_hold_minutes,
         profit_factor, sharpe_ratio, analysis_period_days)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (combination_id) DO UPDATE SET
            sl_pct = EXCLUDED.sl_pct,
            ts_activation_pct = EXCLUDED.ts_activation_pct,
            ts_callback_pct = EXCLUDED.ts_callback_pct,
            total_signals = EXCLUDED.total_signals,
            winning_trades = EXCLUDED.winning_trades,
            losing_trades = EXCLUDED.losing_trades,
            win_rate = EXCLUDED.win_rate,
            total_pnl_pct = EXCLUDED.total_pnl_pct,
            avg_pnl_pct = EXCLUDED.avg_pnl_pct,
            avg_win_pct = EXCLUDED.avg_win_pct,
            avg_loss_pct = EXCLUDED.avg_loss_pct,
            max_profit_pct = EXCLUDED.max_profit_pct,
            max_drawdown_pct = EXCLUDED.max_drawdown_pct,
            avg_hold_minutes = EXCLUDED.avg_hold_minutes,
            profit_factor = EXCLUDED.profit_factor,
            sharpe_ratio = EXCLUDED.sharpe_ratio,
            analysis_period_days = EXCLUDED.analysis_period_days,
            updated_at = NOW()
    """
    
    db.execute_update(query, (
        combination_id,
        params['sl_pct'],
        params['ts_activation_pct'],
        params['ts_callback_pct'],
        int(params['total_signals']),
        int(params['winning_trades']),
        int(params['losing_trades']),
        params['win_rate'],
        params['total_pnl_pct'],
        params['avg_pnl_pct'],
        params['avg_win_pct'],
        params['avg_loss_pct'],
        params['max_profit_pct'],
        params['max_drawdown_pct'],
        params['avg_hold_minutes'],
        params.get('profit_factor'),
        params.get('sharpe_ratio'),
        days
    ))


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Aggregate results and find best parameters')
    parser.add_argument('--days', type=int, default=30,
                       help='Analysis period in days (default: 30)')
    parser.add_argument('--min-win-rate', type=float, default=60.0,
                       help='Minimum win rate for best params (default: 60.0)')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("AGGREGATE RESULTS AND FIND BEST PARAMETERS")
    logger.info("="*120)
    logger.info(f"Analysis period: {args.days} days")
    logger.info(f"Min win rate: {args.min_win_rate}%\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load combinations
    logger.info("Step 1: Loading combinations...")
    combinations = load_combinations(db)
    logger.info(f"Found {len(combinations)} combinations\n")
    
    if not combinations:
        logger.warning("No combinations found. Run 1_discover_combinations.py first.")
        db.close()
        return
    
    # Process each combination
    logger.info("Step 2: Aggregating results...")
    processed = 0
    
    for idx, combo in enumerate(combinations, 1):
        combo_id = combo['id']
        combo_name = combo['combination_name']
        
        logger.info(f"[{idx}/{len(combinations)}] {combo_name}")
        
        # Aggregate
        df = aggregate_for_combination(db, combo_id)
        
        if df is None or len(df) == 0:
            logger.warning(f"  No simulation results found")
            continue
        
        logger.info(f"  Found {len(df)} parameter sets")
        
        # Find best
        best = find_best_parameters(df, args.min_win_rate)
        
        if best is None:
            logger.warning(f"  No valid parameters found")
            continue
        
        logger.info(f"  Best: SL={best['sl_pct']}, TS_ACT={best['ts_activation_pct']}, "
                   f"TS_CB={best['ts_callback_pct']}, WR={best['win_rate']:.1f}%, "
                   f"Total PnL={best['total_pnl_pct']:.1f}%")
        
        # Save
        save_best_parameters(db, combo_id, best, args.days)
        processed += 1
    
    # Summary
    print("\n" + "="*120)
    print("SUMMARY")
    print("="*120)
    print(f"Combinations processed: {processed}/{len(combinations)}")
    print("="*120)
    
    # Close connection
    db.close()
    logger.info("\n✅ Aggregation complete!")


if __name__ == "__main__":
    main()
