#!/usr/bin/env python3
"""
Detailed Analysis of Yesterday's Signals
Shows exit type (SL/TS/Timeout) and PnL for each signal
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_yesterday_results(db: DatabaseHelper):
    """Get all yesterday signals with their simulation results"""
    
    query = """
        SELECT 
            ys.signal_id,
            ys.pair_symbol,
            ys.entry_time,
            ys.signal_type,
            ys.sl_pct,
            ys.ts_activation_pct,
            ys.ts_callback_pct,
            yr.exit_type,
            yr.exit_price,
            yr.pnl_pct,
            yr.hold_duration_minutes,
            yr.max_profit_pct,
            yr.max_drawdown_pct
        FROM optimization.yesterday_signals ys
        LEFT JOIN optimization.yesterday_results yr ON yr.signal_id = ys.signal_id
        ORDER BY ys.entry_time
    """
    
    return db.execute_query(query)


def classify_exit_type(exit_type):
    """Classify exit type with emoji"""
    if exit_type == 'SL':
        return '🛑 Stop Loss'
    elif exit_type == 'TS':
        return '✅ Trailing Stop'
    elif exit_type == 'TIME_LIMIT':
        return '⏱️  Timeout'
    else:
        return '❓ Unknown'


def format_pnl(pnl_pct):
    """Format PnL with color coding"""
    if pnl_pct is None:
        return 'N/A'
    
    if pnl_pct > 0:
        return f'+{pnl_pct:.2f}%'
    else:
        return f'{pnl_pct:.2f}%'


def print_detailed_analysis(signals):
    """Print detailed analysis for each signal"""
    
    print("\n" + "="*120)
    print("📊 DETAILED YESTERDAY SIGNALS ANALYSIS")
    print("="*120)
    
    # Statistics
    total_signals = len(signals)
    simulated = sum(1 for s in signals if s['exit_type'] is not None)
    not_simulated = total_signals - simulated
    
    # Exit type breakdown
    sl_count = sum(1 for s in signals if s['exit_type'] == 'SL')
    ts_count = sum(1 for s in signals if s['exit_type'] == 'TS')
    timeout_count = sum(1 for s in signals if s['exit_type'] == 'TIME_LIMIT')
    
    # PnL statistics
    total_pnl = sum(s['pnl_pct'] or 0 for s in signals if s['pnl_pct'] is not None)
    winning = sum(1 for s in signals if s['pnl_pct'] and s['pnl_pct'] > 0)
    losing = sum(1 for s in signals if s['pnl_pct'] and s['pnl_pct'] <= 0)
    
    print(f"\n📈 SUMMARY:")
    print(f"  Total Signals: {total_signals}")
    print(f"  Simulated: {simulated}")
    print(f"  Not Simulated: {not_simulated}")
    print(f"\n🎯 EXIT TYPES:")
    print(f"  🛑 Stop Loss: {sl_count} ({sl_count/simulated*100:.1f}%)" if simulated > 0 else "  🛑 Stop Loss: 0")
    print(f"  ✅ Trailing Stop: {ts_count} ({ts_count/simulated*100:.1f}%)" if simulated > 0 else "  ✅ Trailing Stop: 0")
    print(f"  ⏱️  Timeout: {timeout_count} ({timeout_count/simulated*100:.1f}%)" if simulated > 0 else "  ⏱️  Timeout: 0")
    print(f"\n💰 PERFORMANCE:")
    print(f"  Total PnL: {format_pnl(total_pnl)}")
    print(f"  Winning Trades: {winning} ({winning/simulated*100:.1f}%)" if simulated > 0 else "  Winning Trades: 0")
    print(f"  Losing Trades: {losing} ({losing/simulated*100:.1f}%)" if simulated > 0 else "  Losing Trades: 0")
    if simulated > 0:
        print(f"  Win Rate: {winning/simulated*100:.1f}%")
        print(f"  Avg PnL per Trade: {total_pnl/simulated:.2f}%")
    
    # Detailed table
    print("\n" + "="*120)
    print("📋 DETAILED SIGNAL BREAKDOWN:")
    print("="*120)
    
    header = (
        f"{'#':<4} {'Symbol':<12} {'Type':<6} {'Entry Time':<20} "
        f"{'Exit Type':<18} {'PnL':<10} {'Duration':<10} {'Params (SL/TS_Act/TS_Cb)'}"
    )
    print(header)
    print("-"*120)
    
    for i, signal in enumerate(signals, 1):
        if signal['exit_type'] is None:
            # Not simulated yet
            row = (
                f"{i:<4} {signal['pair_symbol']:<12} {signal['signal_type']:<6} "
                f"{signal['entry_time'].strftime('%Y-%m-%d %H:%M'):<20} "
                f"{'⚠️  Not Simulated':<18} {'N/A':<10} {'N/A':<10} "
                f"{signal['sl_pct']:.1f}% / {signal['ts_activation_pct']:.1f}% / {signal['ts_callback_pct']:.1f}%"
            )
        else:
            exit_label = classify_exit_type(signal['exit_type'])
            pnl_str = format_pnl(signal['pnl_pct'])
            duration = f"{signal['hold_duration_minutes']}m" if signal['hold_duration_minutes'] else 'N/A'
            
            row = (
                f"{i:<4} {signal['pair_symbol']:<12} {signal['signal_type']:<6} "
                f"{signal['entry_time'].strftime('%Y-%m-%d %H:%M'):<20} "
                f"{exit_label:<18} {pnl_str:<10} {duration:<10} "
                f"{signal['sl_pct']:.1f}% / {signal['ts_activation_pct']:.1f}% / {signal['ts_callback_pct']:.1f}%"
            )
        
        print(row)
    
    print("="*120)
    
    # Detailed breakdown by exit type
    print("\n📊 BREAKDOWN BY EXIT TYPE:\n")
    
    # Stop Loss signals
    sl_signals = [s for s in signals if s['exit_type'] == 'SL']
    if sl_signals:
        print(f"🛑 STOP LOSS ({len(sl_signals)} signals):")
        total_sl_loss = sum(s['pnl_pct'] for s in sl_signals)
        print(f"   Total Loss: {format_pnl(total_sl_loss)}")
        print(f"   Avg Loss: {total_sl_loss/len(sl_signals):.2f}%")
        print(f"   Signals: {', '.join(s['pair_symbol'] for s in sl_signals[:10])}" + 
              (f" ... (+{len(sl_signals)-10} more)" if len(sl_signals) > 10 else ""))
    
    # Trailing Stop signals
    ts_signals = [s for s in signals if s['exit_type'] == 'TS']
    if ts_signals:
        print(f"\n✅ TRAILING STOP ({len(ts_signals)} signals):")
        total_ts_profit = sum(s['pnl_pct'] for s in ts_signals)
        print(f"   Total Profit: {format_pnl(total_ts_profit)}")
        print(f"   Avg Profit: {total_ts_profit/len(ts_signals):.2f}%")
        print(f"   Signals: {', '.join(s['pair_symbol'] for s in ts_signals[:10])}" + 
              (f" ... (+{len(ts_signals)-10} more)" if len(ts_signals) > 10 else ""))
    
    # Timeout signals
    timeout_signals = [s for s in signals if s['exit_type'] == 'TIME_LIMIT']
    if timeout_signals:
        print(f"\n⏱️  TIMEOUT ({len(timeout_signals)} signals):")
        total_timeout_pnl = sum(s['pnl_pct'] for s in timeout_signals)
        timeout_positive = sum(1 for s in timeout_signals if s['pnl_pct'] > 0)
        timeout_negative = sum(1 for s in timeout_signals if s['pnl_pct'] <= 0)
        print(f"   Total PnL: {format_pnl(total_timeout_pnl)}")
        print(f"   Avg PnL: {total_timeout_pnl/len(timeout_signals):.2f}%")
        print(f"   Positive: {timeout_positive}, Negative: {timeout_negative}")
        print(f"   Signals: {', '.join(s['pair_symbol'] for s in timeout_signals[:10])}" + 
              (f" ... (+{len(timeout_signals)-10} more)" if len(timeout_signals) > 10 else ""))
    
    print("\n" + "="*120)


def main():
    """Main execution"""
    logger.info("Starting detailed yesterday signals analysis...")
    
    db = DatabaseHelper()
    db.connect()
    
    try:
        # Get results
        signals = get_yesterday_results(db)
        
        if not signals:
            logger.warning("No signals found in yesterday_signals table")
            logger.info("Run 1_select_yesterday_signals.py first")
            return
        
        # Print analysis
        print_detailed_analysis(signals)
        
        logger.info(f"\n✅ Analysis complete! Analyzed {len(signals)} signals")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
