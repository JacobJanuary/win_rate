#!/usr/bin/env python3
"""
Backtest Trading with Optimized Parameters
Simulates trading using best parameters from optimization (total_pnl > 180%)
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


def get_signals_with_best_params(db: DatabaseHelper, min_total_pnl: float = 180):
    """
    Get signals with their best optimized parameters
    
    For each signal, finds the best strategy (highest total_pnl > threshold)
    """
    
    query = f"""
        WITH best_strategies AS (
            -- For each unique strategy combination, get the best parameters
            SELECT DISTINCT ON (strategy_name, signal_type, market_regime)
                strategy_name,
                signal_type,
                market_regime,
                sl_pct,
                ts_activation_pct,
                ts_callback_pct,
                total_pnl_pct,
                win_rate,
                total_signals
            FROM optimization.best_parameters
            WHERE total_pnl_pct > {min_total_pnl}
            ORDER BY 
                strategy_name, 
                signal_type, 
                market_regime,
                total_pnl_pct DESC
        ),
        signals_with_params AS (
            SELECT 
                ss.signal_id,
                ss.pair_symbol,
                ss.signal_timestamp,
                ss.entry_time,
                ss.signal_type,
                ss.strategy_name,
                ss.market_regime,
                
                -- Best params for this strategy
                bs.sl_pct,
                bs.ts_activation_pct,
                bs.ts_callback_pct,
                bs.total_pnl_pct as strategy_total_pnl,
                bs.win_rate as strategy_win_rate,
                
                -- Simulation results with these params
                sr.exit_type,
                sr.exit_price,
                sr.exit_time,
                sr.pnl_pct,
                sr.max_profit_pct,
                sr.max_drawdown_pct,
                sr.hold_duration_minutes
                
            FROM optimization.selected_signals ss
            JOIN best_strategies bs ON (
                bs.strategy_name = ss.strategy_name
                AND bs.signal_type = ss.signal_type
                AND bs.market_regime = ss.market_regime
            )
            LEFT JOIN optimization.simulation_results sr ON (
                sr.signal_id = ss.signal_id
                AND sr.sl_pct = bs.sl_pct
                AND sr.ts_activation_pct = bs.ts_activation_pct
                AND sr.ts_callback_pct = bs.ts_callback_pct
            )
        )
        SELECT * FROM signals_with_params
        WHERE pnl_pct IS NOT NULL  -- Only signals with simulation results
        ORDER BY signal_timestamp
    """
    
    return db.execute_query(query)


def calculate_portfolio_metrics(results):
    """Calculate portfolio-level metrics"""
    
    df = pd.DataFrame(results)
    
    # Convert to numeric
    numeric_cols = ['pnl_pct', 'max_profit_pct', 'max_drawdown_pct', 
                   'hold_duration_minutes', 'strategy_total_pnl', 'strategy_win_rate']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Portfolio metrics
    total_trades = len(df)
    winning_trades = len(df[df['pnl_pct'] > 0])
    losing_trades = len(df[df['pnl_pct'] <= 0])
    
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
    
    avg_win = df[df['pnl_pct'] > 0]['pnl_pct'].mean() if winning_trades > 0 else 0
    avg_loss = df[df['pnl_pct'] <= 0]['pnl_pct'].mean() if losing_trades > 0 else 0
    
    profit_factor = abs(avg_win * winning_trades / (avg_loss * losing_trades)) if (losing_trades > 0 and avg_loss != 0) else None
    
    total_pnl = df['pnl_pct'].sum()
    avg_pnl = df['pnl_pct'].mean()
    
    max_drawdown = df['max_drawdown_pct'].min()
    avg_hold_hours = df['hold_duration_minutes'].mean() / 60
    
    # Exit type breakdown
    exit_breakdown = df['exit_type'].value_counts()
    
    # By strategy
    strategy_stats = df.groupby('strategy_name').agg({
        'pnl_pct': ['count', 'sum', 'mean'],
        'signal_id': 'count'
    }).round(2)
    
    return {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_factor': profit_factor,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'max_drawdown': max_drawdown,
        'avg_hold_hours': avg_hold_hours,
        'exit_breakdown': exit_breakdown,
        'strategy_stats': strategy_stats,
        'df': df
    }


def print_backtest_report(metrics, min_pnl_threshold):
    """Print detailed backtest report"""
    
    print("\n" + "=" * 120)
    print(f"BACKTEST REPORT - Optimized Parameters (Strategy Total PnL > {min_pnl_threshold}%)")
    print("=" * 120)
    
    print(f"\n📊 PORTFOLIO PERFORMANCE:")
    print(f"   Total Trades: {metrics['total_trades']}")
    print(f"   Winning: {metrics['winning_trades']} ({metrics['winning_trades']/metrics['total_trades']*100:.1f}%)")
    print(f"   Losing:  {metrics['losing_trades']} ({metrics['losing_trades']/metrics['total_trades']*100:.1f}%)")
    print(f"   Win Rate: {metrics['win_rate']:.2f}%")
    
    print(f"\n💰 PnL METRICS:")
    print(f"   Total PnL: {metrics['total_pnl']:.2f}%")
    print(f"   Average PnL per Trade: {metrics['avg_pnl']:.2f}%")
    print(f"   Average Win: {metrics['avg_win']:.2f}%")
    print(f"   Average Loss: {metrics['avg_loss']:.2f}%")
    if metrics['profit_factor']:
        print(f"   Profit Factor: {metrics['profit_factor']:.2f}")
    
    print(f"\n📉 RISK METRICS:")
    print(f"   Max Drawdown: {metrics['max_drawdown']:.2f}%")
    print(f"   Average Hold Time: {metrics['avg_hold_hours']:.1f} hours")
    
    print(f"\n🚪 EXIT TYPE BREAKDOWN:")
    for exit_type, count in metrics['exit_breakdown'].items():
        pct = count / metrics['total_trades'] * 100
        print(f"   {exit_type:12}: {count:4} ({pct:5.1f}%)")
    
    print(f"\n📈 TOP-10 STRATEGIES BY TOTAL PnL:")
    print("-" * 120)
    
    df = metrics['df']
    strategy_summary = df.groupby('strategy_name').agg({
        'pnl_pct': ['sum', 'mean', 'count'],
        'strategy_total_pnl': 'first',
        'strategy_win_rate': 'first'
    }).round(2)
    
    strategy_summary.columns = ['total_pnl', 'avg_pnl', 'trades', 'strategy_pnl', 'strategy_wr']
    strategy_summary = strategy_summary.sort_values('total_pnl', ascending=False).head(10)
    
    print(f"{'#':<3} {'Strategy':<50} {'Trades':<8} {'Total PnL':<12} {'Avg PnL':<10} {'Strat PnL':<12}")
    print("-" * 120)
    for idx, (strategy, row) in enumerate(strategy_summary.iterrows(), 1):
        strategy_short = strategy[:47] + '...' if len(strategy) > 50 else strategy
        print(f"{idx:<3} {strategy_short:<50} {int(row['trades']):<8} "
              f"{row['total_pnl']:>10.2f}% {row['avg_pnl']:>8.2f}% {row['strategy_pnl']:>10.2f}%")
    
    print("=" * 120)


def save_results(df, min_pnl_threshold):
    """Save backtest results to CSV"""
    
    output_file = f'results/backtest_optimized_pnl{int(min_pnl_threshold)}.csv'
    os.makedirs('results', exist_ok=True)
    
    df.to_csv(output_file, index=False)
    logger.info(f"✅ Saved backtest results to {output_file}")


def simulate_capital_flow(df, position_size=100):
    """
    Simulate capital flow with frozen/free balance
    
    Args:
        df: DataFrame with trade results
        position_size: Size of each position in USD
    
    Returns:
        Dict with daily balance, capital requirements, final balance
    """
    
    # Convert timestamps to datetime
    df['entry_dt'] = pd.to_datetime(df['entry_time'])
    df['exit_dt'] = pd.to_datetime(df['exit_time'], unit='ms')
    
    # Create events list (open/close)
    events = []
    
    for idx, row in df.iterrows():
        # Entry event
        events.append({
            'timestamp': row['entry_dt'],
            'type': 'OPEN',
            'signal_id': row['signal_id'],
            'pair': row['pair_symbol'],
            'amount': -position_size  # Freeze capital
        })
        
        # Exit event
        pnl = position_size * (row['pnl_pct'] / 100)
        events.append({
            'timestamp': row['exit_dt'],
            'type': 'CLOSE',
            'signal_id': row['signal_id'],
            'pair': row['pair_symbol'],
            'amount': position_size + pnl,  # Return capital + PnL
            'pnl': pnl,
            'exit_type': row['exit_type']
        })
    
    # Sort events by timestamp
    events_df = pd.DataFrame(events).sort_values('timestamp')
    
    # Simulate balance flow
    balance = 0
    frozen = 0
    min_balance = 0
    
    daily_stats = []
    current_date = None
    daily_trades_open = 0
    daily_trades_closed = 0
    
    for idx, event in events_df.iterrows():
        event_date = event['timestamp'].date()
        
        # New day - save previous day stats
        if current_date and event_date != current_date:
            daily_stats.append({
                'date': current_date,
                'trades_opened': daily_trades_open,
                'trades_closed': daily_trades_closed,
                'frozen': frozen,
                'free_balance': balance,
                'total_balance': balance + frozen
            })
            daily_trades_open = 0
            daily_trades_closed = 0
        
        current_date = event_date
        
        if event['type'] == 'OPEN':
            balance += event['amount']  # -100
            frozen -= event['amount']    # +100
            daily_trades_open += 1
        else:  # CLOSE
            balance += event['amount']   # +100 + PnL
            frozen -= position_size       # -100
            daily_trades_closed += 1
        
        # Track minimum balance (capital requirement)
        min_balance = min(min_balance, balance)
    
    # Last day stats
    if current_date:
        daily_stats.append({
            'date': current_date,
            'trades_opened': daily_trades_open,
            'trades_closed': daily_trades_closed,
            'frozen': frozen,
            'free_balance': balance,
            'total_balance': balance + frozen
        })
    
    return {
        'daily_stats': pd.DataFrame(daily_stats),
        'capital_required': abs(min_balance),
        'final_balance': balance,
        'final_frozen': frozen,
        'total_final': balance + frozen
    }


def print_capital_report(capital_sim, position_size):
    """Print capital simulation report"""
    
    print("\n" + "=" * 120)
    print("CAPITAL FLOW ANALYSIS")
    print("=" * 120)
    
    print(f"\n💵 POSITION SIZE: ${position_size} per trade")
    
    print(f"\n📊 DAILY BALANCE REPORT:")
    print("-" * 120)
    print(f"{'Date':<12} {'Opened':<8} {'Closed':<8} {'Frozen $':<12} {'Free $':<12} {'Total $':<12}")
    print("-" * 120)
    
    for idx, row in capital_sim['daily_stats'].iterrows():
        print(f"{str(row['date']):<12} {row['trades_opened']:<8} {row['trades_closed']:<8} "
              f"{row['frozen']:>10.2f} {row['free_balance']:>10.2f} {row['total_balance']:>10.2f}")
    
    print("-" * 120)
    
    print(f"\n💰 CAPITAL REQUIREMENTS:")
    print(f"   Required Capital (Max Drawdown): ${capital_sim['capital_required']:,.2f}")
    print(f"   Recommended Capital (with buffer): ${capital_sim['capital_required'] * 1.2:,.2f}")
    
    print(f"\n📈 FINAL RESULTS:")
    print(f"   Free Balance: ${capital_sim['final_balance']:,.2f}")
    print(f"   Frozen in Open Positions: ${capital_sim['final_frozen']:,.2f}")
    print(f"   Total Balance: ${capital_sim['total_final']:,.2f}")
    
    if capital_sim['capital_required'] > 0:
        roi = (capital_sim['total_final'] / capital_sim['capital_required'] - 1) * 100
        print(f"   ROI (on required capital): {roi:,.2f}%")
    
    print("=" * 120)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Backtest with optimized parameters')
    parser.add_argument('--min-pnl', type=float, default=180,
                       help='Minimum strategy total_pnl_pct threshold (default: 180)')
    parser.add_argument('--position-size', type=float, default=100,
                       help='Position size in USD (default: 100)')
    args = parser.parse_args()
    
    logger.info("=" * 120)
    logger.info("BACKTEST WITH OPTIMIZED PARAMETERS")
    logger.info("=" * 120)
    logger.info(f"Strategy filter: total_pnl > {args.min_pnl}%")
    logger.info(f"Position size: ${args.position_size}\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Get signals with best params
    logger.info(f"Loading signals with optimized parameters (strategy total_pnl > {args.min_pnl}%)...")
    results = get_signals_with_best_params(db, args.min_pnl)
    
    if not results:
        logger.warning(f"No signals found with strategy total_pnl > {args.min_pnl}%")
        db.close()
        return
    
    logger.info(f"Found {len(results)} trades to simulate\n")
    
    # Calculate metrics
    logger.info("Calculating portfolio metrics...")
    metrics = calculate_portfolio_metrics(results)
    
    # Print report
    print_backtest_report(metrics, args.min_pnl)
    
    # Capital flow simulation
    logger.info("\nSimulating capital flow...")
    capital_sim = simulate_capital_flow(metrics['df'], args.position_size)
    print_capital_report(capital_sim, args.position_size)
    
    # Save results
    save_results(metrics['df'], args.min_pnl)
    
    # Save daily balance
    daily_file = f'results/daily_balance_pnl{int(args.min_pnl)}.csv'
    capital_sim['daily_stats'].to_csv(daily_file, index=False)
    logger.info(f"✅ Saved daily balance to {daily_file}")
    
    db.close()
    logger.info("\n✅ Backtest complete!")


if __name__ == "__main__":
    main()
