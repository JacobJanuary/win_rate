#!/usr/bin/env python3
"""
Aggregate Simulation Results and Find Best Parameters
Step 4 of optimization pipeline
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


def aggregate_by_strategy_and_params(db: DatabaseHelper):
    """Aggregate results by strategy and parameter combination"""
    
    query = """
        WITH strategy_results AS (
            SELECT 
                ss.strategy_name,
                ss.signal_type,
                ss.market_regime,
                sr.sl_pct,
                sr.ts_activation_pct,
                sr.ts_callback_pct,
                COUNT(*) as total_signals,
                SUM(CASE WHEN sr.pnl_pct > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN sr.pnl_pct <= 0 THEN 1 ELSE 0 END) as losing_trades,
                AVG(sr.pnl_pct) as avg_pnl_pct,
                SUM(sr.pnl_pct) as total_pnl_pct,
                MIN(sr.max_drawdown_pct) as max_drawdown_pct,
                AVG(sr.hold_duration_minutes) as avg_hold_minutes,
                AVG(CASE WHEN sr.pnl_pct > 0 THEN sr.pnl_pct END) as avg_win_pct,
                AVG(CASE WHEN sr.pnl_pct <= 0 THEN sr.pnl_pct END) as avg_loss_pct
            FROM optimization.simulation_results sr
            JOIN optimization.selected_signals ss ON ss.signal_id = sr.signal_id
            GROUP BY 
                ss.strategy_name,
                ss.signal_type,
                ss.market_regime,
                sr.sl_pct,
                sr.ts_activation_pct,
                sr.ts_callback_pct
        )
        SELECT 
            *,
            CASE 
                WHEN total_signals > 0 THEN (winning_trades::DECIMAL / total_signals * 100)
                ELSE 0 
            END as win_rate,
            CASE 
                WHEN avg_loss_pct != 0 AND avg_loss_pct IS NOT NULL 
                THEN ABS(avg_win_pct * winning_trades / (avg_loss_pct * losing_trades))
                ELSE NULL
            END as profit_factor
        FROM strategy_results
        WHERE total_signals >= 10  -- Minimum for statistical significance
        ORDER BY total_pnl_pct DESC
    """
    
    return db.execute_query(query)


def save_best_parameters(db: DatabaseHelper, aggregated_results):
    """Save best parameters to database"""
    
    logger.info("Inserting best parameters into database...")
    
    insert_data = []
    for result in aggregated_results:
        insert_data.append((
            result['strategy_name'],
            result['signal_type'],
            result['market_regime'],
            result['sl_pct'],
            result['ts_activation_pct'],
            result['ts_callback_pct'],
            result['total_signals'],
            result['winning_trades'],
            result['losing_trades'],
            result['win_rate'],
            result['avg_pnl_pct'],
            result['total_pnl_pct'],
            result['max_drawdown_pct'],
            result['profit_factor']
        ))
    
    # Bulk insert
    logger.info("Inserting best parameters into database...")
    try:
        count = db.bulk_insert_best_params(
            'optimization.best_parameters',
            ['strategy_name', 'signal_type', 'market_regime', 'sl_pct', 'ts_activation_pct', 
             'ts_callback_pct', 'total_signals', 'winning_trades', 'losing_trades', 
             'win_rate', 'avg_pnl_pct', 'total_pnl_pct', 'max_drawdown_pct', 
             'profit_factor'],
            insert_data
        )
        logger.info(f"✅ Inserted/Updated {count} parameter combinations")
    except Exception as e:
        logger.error(f"Error inserting best parameters: {e}")
        
def main():
    logger.info("=" * 100)
    logger.info("RESULTS AGGREGATION")
    logger.info("=" * 100)
    logger.info("")
    
    # Connect to database
    logger.info("Step 1: Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Aggregate results
    logger.info("\nStep 2: Aggregating results by strategy and parameters...")
    results = aggregate_by_strategy_and_params(db)
    logger.info(f"Found {len(results)} unique strategy-parameter combinations")
    
    if not results:
        logger.warning("No results to aggregate!")
        db.close()
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Convert Decimal types to float for pandas operations
    numeric_cols = ['sl_pct', 'ts_activation_pct', 'ts_callback_pct', 'win_rate', 
                   'avg_pnl_pct', 'total_pnl_pct', 'max_drawdown_pct', 'profit_factor']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Save to CSV
    output_file = 'results/aggregated_results.csv'
    os.makedirs('results', exist_ok=True)
    df.to_csv(output_file, index=False)
    logger.info(f"✅ Saved aggregated results to {output_file}")
    
    # Save best parameters to DB
    logger.info("\nStep 3: Saving best parameters to database...")
    save_best_parameters(db, results)
    
    # Display top results
    logger.info("\n" + "=" * 100)
    logger.info("TOP-20 PARAMETER COMBINATIONS (by Total PnL):")
    logger.info("=" * 100)
    
    top20 = df.nlargest(20, 'total_pnl_pct')
    
    print(f"\n{'#':<3} {'Strategy':<40} {'SL':>4} {'TS_A':>5} {'TS_C':>5} {'WR%':>6} {'Total_PnL':>10} {'Signals':>8}")
    print('-' * 100)
    
    for idx, (i, row) in enumerate(top20.iterrows(), 1):
        strategy_short = row['strategy_name'][:37] + '...' if len(row['strategy_name']) > 40 else row['strategy_name']
        print(f"{idx:<3} {strategy_short:<40} "
              f"{row['sl_pct']:>4.1f} {row['ts_activation_pct']:>5.1f} {row['ts_callback_pct']:>5.1f} "
              f"{row['win_rate']:>5.1f}% {row['total_pnl_pct']:>9.2f}% {row['total_signals']:>8}")
    
    logger.info("\n" + "=" * 100)
    logger.info("SUMMARY STATISTICS:")
    logger.info("=" * 100)
    logger.info(f"Total combinations analyzed: {len(df)}")
    logger.info(f"Average Win Rate: {df['win_rate'].mean():.2f}%")
    logger.info(f"Average Total PnL: {df['total_pnl_pct'].mean():.2f}%")
    logger.info(f"Best Total PnL: {df['total_pnl_pct'].max():.2f}%")
    logger.info(f"Best Win Rate: {df['win_rate'].max():.2f}%")
    logger.info("=" * 100)
    
    db.close()


if __name__ == "__main__":
    main()
