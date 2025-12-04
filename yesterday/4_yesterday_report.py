#!/usr/bin/env python3
"""
Step 4: Generate Yesterday's Performance Report
Analyze results and calculate capital requirements with leverage 10x
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import pandas as pd
from datetime import datetime
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_yesterday_results(db: DatabaseHelper):
    """Get all simulation results with signal details"""
    
    query = """
        SELECT 
            ys.signal_id,
            ys.pair_symbol,
            ys.signal_timestamp,
            ys.entry_time,
            ys.signal_type,
            ys.strategy_name,
            ys.market_regime,
            ys.sl_pct,
            ys.ts_activation_pct,
            ys.ts_callback_pct,
            yr.exit_type,
            yr.exit_price,
            yr.exit_time,
            yr.pnl_pct,
            yr.max_profit_pct,
            yr.max_drawdown_pct,
            yr.hold_duration_minutes
        FROM optimization.yesterday_results yr
        JOIN optimization.yesterday_signals ys ON ys.signal_id = yr.signal_id
        ORDER BY ys.signal_timestamp
    """
    
    return db.execute_query(query)


def calculate_capital_flow(df, position_size=100, leverage=10):
    """Calculate capital requirements with leverage"""
    
    margin_per_position = position_size / leverage
    
    # Convert timestamps
    df['entry_dt'] = pd.to_datetime(df['entry_time']).dt.tz_localize(None)
    df['exit_dt'] = pd.to_datetime(df['exit_time'], unit='ms').dt.tz_localize(None)
    
    # Create events
    events = []
    
    for idx, row in df.iterrows():
        # Entry
        events.append({
            'timestamp': row['entry_dt'],
            'type': 'OPEN',
            'amount': -margin_per_position
        })
        
        # Exit
        pnl = position_size * (row['pnl_pct'] / 100)
        events.append({
            'timestamp': row['exit_dt'],
            'type': 'CLOSE',
            'amount': margin_per_position + pnl
        })
    
    # Sort and simulate
    events_df = pd.DataFrame(events).sort_values('timestamp')
    
    balance = 0
    frozen = 0
    min_balance = 0
    
    for idx, event in events_df.iterrows():
        if event['type'] == 'OPEN':
            balance += event['amount']
            frozen -= event['amount']
        else:
            balance += event['amount']
            frozen -= margin_per_position
        
        min_balance = min(min_balance, balance)
    
    return {
        'capital_required': abs(min_balance),
        'final_balance': balance,
        'final_frozen': frozen
    }


def print_report(df, capital, position_size, leverage):
    """Print performance report"""
    
    # Convert numeric columns
    numeric_cols = ['pnl_pct', 'max_profit_pct', 'max_drawdown_pct', 'hold_duration_minutes']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print("\n" + "=" * 120)
    print("YESTERDAY PERFORMANCE REPORT")
    print("=" * 120)
    
    # Time window
    min_time = df['signal_timestamp'].min()
    max_time = df['signal_timestamp'].max()
    print(f"\n📅 TIME WINDOW:")
    print(f"   {min_time} to {max_time}")
    
    # Trading params
    margin = position_size / leverage
    print(f"\n💵 TRADING PARAMETERS:")
    print(f"   Position Size: ${position_size}")
    print(f"   Leverage: {leverage}x")
    print(f"   Margin per trade: ${margin:.2f}")
    
    # Performance
    total = len(df)
    winning = len(df[df['pnl_pct'] > 0])
    losing = len(df[df['pnl_pct'] <= 0])
    win_rate = (winning / total * 100) if total > 0 else 0
    
    print(f"\n📊 PERFORMANCE:")
    print(f"   Total Trades: {total}")
    print(f"   Winning: {winning} ({win_rate:.1f}%)")
    print(f"   Losing: {losing} ({100-win_rate:.1f}%)")
    
    # PnL
    total_pnl_pct = df['pnl_pct'].sum()
    avg_pnl = df['pnl_pct'].mean()
    avg_win = df[df['pnl_pct'] > 0]['pnl_pct'].mean() if winning > 0 else 0
    avg_loss = df[df['pnl_pct'] <= 0]['pnl_pct'].mean() if losing > 0 else 0
    
    print(f"\n💰 PnL METRICS:")
    print(f"   Total PnL: {total_pnl_pct:.2f}%")
    print(f"   Avg PnL: {avg_pnl:.2f}%")
    print(f"   Avg Win: {avg_win:.2f}%")
    print(f"   Avg Loss: {avg_loss:.2f}%")
    
    total_profit_usd = total * position_size * (avg_pnl / 100)
    print(f"   Total Profit (USD): ${total_profit_usd:.2f}")
    
    # Exit types
    print(f"\n🚪 EXIT TYPE BREAKDOWN:")
    exit_counts = df['exit_type'].value_counts()
    for exit_type, count in exit_counts.items():
        pct = count / total * 100
        print(f"   {exit_type:12}: {count:4} ({pct:5.1f}%)")
    
    # Capital
    print(f"\n💼 CAPITAL REQUIREMENTS:")
    print(f"   Required Capital (with {leverage}x leverage): ${capital['capital_required']:,.2f}")
    print(f"   Recommended (with 20% buffer): ${capital['capital_required'] * 1.2:,.2f}")
    print(f"   Final Balance: ${capital['final_balance']:,.2f}")
    print(f"   Frozen: ${capital['final_frozen']:,.2f}")
    
    if capital['capital_required'] > 0:
        roi = (capital['final_balance'] / capital['capital_required']) * 100
        print(f"   ROI (on required capital): {roi:.2f}%")
    
    # Top strategies
    print(f"\n📈 TOP-10 STRATEGIES:")
    print("-" * 120)
    
    strategy_stats = df.groupby('strategy_name').agg({
        'pnl_pct': ['sum', 'mean', 'count']
    }).round(2)
    strategy_stats.columns = ['total_pnl', 'avg_pnl', 'trades']
    strategy_stats = strategy_stats.sort_values('total_pnl', ascending=False).head(10)
    
    print(f"{'#':<3} {'Strategy':<60} {'Trades':<8} {'Total PnL':<12} {'Avg PnL':<10}")
    print("-" * 120)
    for idx, (strategy, row) in enumerate(strategy_stats.iterrows(), 1):
        strategy_short = strategy[:57] + '...' if len(strategy) > 60 else strategy
        print(f"{idx:<3} {strategy_short:<60} {int(row['trades']):<8} {row['total_pnl']:>10.2f}% {row['avg_pnl']:>8.2f}%")
    
    print("=" * 120)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Yesterday performance report')
    parser.add_argument('--position-size', type=float, default=100,
                       help='Position size in USD (default: 100)')
    parser.add_argument('--leverage', type=float, default=10,
                       help='Leverage (default: 10x)')
    args = parser.parse_args()
    
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    logger.info("Fetching results...")
    results = get_yesterday_results(db)
    
    if not results:
        logger.warning("No results found! Run steps 1-3 first.")
        db.close()
        return
    
    logger.info(f"Found {len(results)} results\n")
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Calculate capital
    logger.info("Calculating capital flow...")
    capital = calculate_capital_flow(df, args.position_size, args.leverage)
    
    # Print report
    print_report(df, capital, args.position_size, args.leverage)
    
    # Save to CSV
    output_file = 'results/yesterday_performance.csv'
    os.makedirs('results', exist_ok=True)
    df.to_csv(output_file, index=False)
    logger.info(f"\n✅ Saved results to {output_file}")
    
    db.close()


if __name__ == "__main__":
    main()
