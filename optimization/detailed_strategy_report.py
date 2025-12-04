#!/usr/bin/env python3
"""
Detailed Strategy Performance Report
Shows breakdown by exit type, profitability, etc.
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


def get_detailed_strategy_stats(db: DatabaseHelper):
    """Get detailed statistics for each strategy including exit type breakdown"""
    
    query = """
        WITH best_params_per_strategy AS (
            SELECT DISTINCT ON (strategy_name, signal_type, market_regime)
                strategy_name,
                signal_type,
                market_regime,
                sl_pct,
                ts_activation_pct,
                ts_callback_pct,
                total_pnl_pct
            FROM optimization.best_parameters
            ORDER BY strategy_name, signal_type, market_regime, total_pnl_pct DESC
        ),
        strategy_details AS (
            SELECT 
                ss.strategy_name,
                ss.signal_type,
                ss.market_regime,
                bp.sl_pct as best_sl,
                bp.ts_activation_pct as best_ts_act,
                bp.ts_callback_pct as best_ts_cb,
                
                -- Total signals
                COUNT(DISTINCT sr.signal_id) as total_signals,
                
                -- Profitability
                SUM(CASE WHEN sr.pnl_pct > 0 THEN 1 ELSE 0 END) as profitable_trades,
                SUM(CASE WHEN sr.pnl_pct <= 0 THEN 1 ELSE 0 END) as losing_trades,
                
                -- Exit type breakdown
                SUM(CASE WHEN sr.exit_type = 'SL' THEN 1 ELSE 0 END) as sl_exits,
                SUM(CASE WHEN sr.exit_type = 'TS' THEN 1 ELSE 0 END) as ts_exits,
                SUM(CASE WHEN sr.exit_type = 'TP' THEN 1 ELSE 0 END) as tp_exits,
                SUM(CASE WHEN sr.exit_type = 'TIME_LIMIT' THEN 1 ELSE 0 END) as timeout_exits,
                
                -- Performance metrics (for best params only)
                AVG(sr.pnl_pct) as avg_pnl,
                SUM(sr.pnl_pct) as total_pnl,
                AVG(sr.hold_duration_minutes) as avg_hold_minutes,
                
                -- Win rate
                (SUM(CASE WHEN sr.pnl_pct > 0 THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100) as win_rate
                
            FROM optimization.selected_signals ss
            JOIN best_params_per_strategy bp ON (
                bp.strategy_name = ss.strategy_name
                AND bp.signal_type = ss.signal_type
                AND bp.market_regime = ss.market_regime
            )
            JOIN optimization.simulation_results sr ON (
                sr.signal_id = ss.signal_id
                AND sr.sl_pct = bp.sl_pct
                AND sr.ts_activation_pct = bp.ts_activation_pct
                AND sr.ts_callback_pct = bp.ts_callback_pct
            )
            GROUP BY 
                ss.strategy_name,
                ss.signal_type,
                ss.market_regime,
                bp.sl_pct,
                bp.ts_activation_pct,
                bp.ts_callback_pct
        )
        SELECT * 
        FROM strategy_details
        ORDER BY total_pnl DESC
    """
    
    return db.execute_query(query)


def print_detailed_report(results):
    """Print formatted detailed report"""
    
    df = pd.DataFrame(results)
    
    # Convert numeric columns
    numeric_cols = ['best_sl', 'best_ts_act', 'best_ts_cb', 'avg_pnl', 'total_pnl', 
                   'avg_hold_minutes', 'win_rate']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print("\n" + "=" * 150)
    print("DETAILED STRATEGY PERFORMANCE REPORT")
    print("=" * 150)
    print(f"\nTotal Strategies Analyzed: {len(df)}\n")
    
    for idx, row in df.iterrows():
        print("=" * 150)
        print(f"Strategy #{idx + 1}: {row['strategy_name'][:80]}")
        print(f"Type: {row['signal_type']:5} | Market: {row['market_regime']:8}")
        print("-" * 150)
        
        print(f"\n📊 BEST PARAMETERS:")
        print(f"   SL: {row['best_sl']:.1f}% | TS Activation: {row['best_ts_act']:.1f}% | TS Callback: {row['best_ts_cb']:.1f}%")
        
        print(f"\n📈 PERFORMANCE:")
        print(f"   Total Signals: {row['total_signals']:4} | Win Rate: {row['win_rate']:.1f}%")
        print(f"   Avg PnL: {row['avg_pnl']:6.2f}% | Total PnL: {row['total_pnl']:8.2f}%")
        print(f"   Avg Hold Time: {row['avg_hold_minutes']:.0f} minutes ({row['avg_hold_minutes']/60:.1f} hours)")
        
        print(f"\n🎯 PROFITABILITY:")
        print(f"   Profitable: {row['profitable_trades']:4} ({row['profitable_trades']/row['total_signals']*100:.1f}%)")
        print(f"   Losing:     {row['losing_trades']:4} ({row['losing_trades']/row['total_signals']*100:.1f}%)")
        
        print(f"\n🚪 EXIT TYPE BREAKDOWN:")
        total = row['total_signals']
        print(f"   Stop Loss (SL):     {row['sl_exits']:4} ({row['sl_exits']/total*100:5.1f}%)")
        print(f"   Trailing Stop (TS): {row['ts_exits']:4} ({row['ts_exits']/total*100:5.1f}%)")
        print(f"   Take Profit (TP):   {row['tp_exits']:4} ({row['tp_exits']/total*100:5.1f}%)")
        print(f"   Timeout:            {row['timeout_exits']:4} ({row['timeout_exits']/total*100:5.1f}%)")
        
        print()
    
    print("=" * 150)
    
    # Summary statistics
    print("\n📊 SUMMARY STATISTICS:")
    print("=" * 150)
    print(f"Average Win Rate: {df['win_rate'].mean():.2f}%")
    print(f"Average Total PnL: {df['total_pnl'].mean():.2f}%")
    print(f"Best Strategy Total PnL: {df['total_pnl'].max():.2f}%")
    print(f"Average Signals per Strategy: {df['total_signals'].mean():.0f}")
    
    print(f"\nExit Type Distribution (Average across all strategies):")
    print(f"  Stop Loss:     {df['sl_exits'].sum() / df['total_signals'].sum() * 100:.1f}%")
    print(f"  Trailing Stop: {df['ts_exits'].sum() / df['total_signals'].sum() * 100:.1f}%")
    print(f"  Take Profit:   {df['tp_exits'].sum() / df['total_signals'].sum() * 100:.1f}%")
    print(f"  Timeout:       {df['timeout_exits'].sum() / df['total_signals'].sum() * 100:.1f}%")
    
    print("=" * 150)


def save_to_csv(results):
    """Save detailed results to CSV"""
    df = pd.DataFrame(results)
    
    # Convert numeric columns
    numeric_cols = ['best_sl', 'best_ts_act', 'best_ts_cb', 'avg_pnl', 'total_pnl', 
                   'avg_hold_minutes', 'win_rate']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    output_file = 'results/detailed_strategy_report.csv'
    os.makedirs('results', exist_ok=True)
    df.to_csv(output_file, index=False)
    logger.info(f"✅ Saved detailed report to {output_file}")


def main():
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    logger.info("Fetching detailed strategy statistics...")
    results = get_detailed_strategy_stats(db)
    
    if not results:
        logger.warning("No data found!")
        db.close()
        return
    
    logger.info(f"Found {len(results)} strategies\n")
    
    # Print detailed report
    print_detailed_report(results)
    
    # Save to CSV
    save_to_csv(results)
    
    db.close()
    logger.info("\n✅ Report generation complete!")


if __name__ == "__main__":
    main()
