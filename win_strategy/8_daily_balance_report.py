#!/usr/bin/env python3
"""
Step 8: Daily Balance Report
Shows daily P&L with capital management

Usage:
    python3 8_daily_balance_report.py
    python3 8_daily_balance_report.py --position-size 100 --leverage 10
    python3 8_daily_balance_report.py --days 30 --initial-capital 200
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CapitalTracker:
    """Track capital, positions, and balance"""
    
    def __init__(self, initial_capital, position_size, leverage, max_positions):
        self.balance = initial_capital
        self.position_size = position_size
        self.leverage = leverage
        self.max_positions = max_positions
        self.total_invested = initial_capital
        self.locked_capital = 0
        self.open_positions = {}  # signal_id -> margin
        self.fee_rate = 0.0004  # 0.04% per trade
    
    def get_margin_required(self):
        """Calculate margin required for one position"""
        return self.position_size / self.leverage
    
    def get_free_capital(self):
        """Calculate free capital"""
        return self.balance - self.locked_capital
    
    def can_open_position(self):
        """Check if we can open a new position"""
        margin = self.get_margin_required()
        free = self.get_free_capital()
        return free >= margin and len(self.open_positions) < self.max_positions
    
    def need_capital(self):
        """Check if we need to add capital"""
        margin = self.get_margin_required()
        free = self.get_free_capital()
        return free < margin
    
    def add_capital(self):
        """Add capital when needed"""
        margin = self.get_margin_required()
        free = self.get_free_capital()
        needed = margin - free + 10  # Add a bit extra
        
        self.balance += needed
        self.total_invested += needed
        return needed
    
    def open_position(self, signal_id):
        """Open a position"""
        margin = self.get_margin_required()
        self.locked_capital += margin
        self.open_positions[signal_id] = margin
    
    def close_position(self, signal_id, pnl_pct):
        """Close a position and return PnL and fee"""
        if signal_id not in self.open_positions:
            return 0, 0
        
        # Calculate PnL
        pnl = self.position_size * (pnl_pct / 100)
        
        # Calculate fee (entry + exit)
        fee = self.position_size * self.fee_rate * 2
        
        # Update balance
        self.balance += pnl - fee
        
        # Release margin
        margin = self.open_positions.pop(signal_id)
        self.locked_capital -= margin
        
        return pnl, fee


def load_all_simulations(db: DatabaseHelper, days: int):
    """Load all simulations with their signals"""
    
    query = f"""
        SELECT 
            cs.id as signal_id,
            cs.signal_timestamp,
            cs.entry_time,
            cs.pair_symbol,
            cs.signal_type,
            csim.pnl_pct,
            csim.exit_time,
            csim.hold_duration_minutes,
            cs.combination_id,
            csim.sl_pct,
            csim.ts_activation_pct,
            csim.ts_callback_pct
        FROM optimization.combination_signals cs
        JOIN optimization.combination_simulations csim ON csim.signal_id = cs.id
        JOIN optimization.combination_best_parameters cbp ON (
            cbp.combination_id = cs.combination_id
            AND CAST(cbp.sl_pct AS NUMERIC(5,2)) = CAST(csim.sl_pct AS NUMERIC(5,2))
            AND CAST(cbp.ts_activation_pct AS NUMERIC(5,2)) = CAST(csim.ts_activation_pct AS NUMERIC(5,2))
            AND CAST(cbp.ts_callback_pct AS NUMERIC(5,2)) = CAST(csim.ts_callback_pct AS NUMERIC(5,2))
        )
        WHERE cs.signal_timestamp >= NOW() - INTERVAL '{days} days'
        ORDER BY cs.signal_timestamp
    """
    
    results = db.execute_query(query)
    
    # Debug: log first few results
    if results:
        logger.info(f"Sample simulation: {results[0]['pair_symbol']} - PnL: {results[0]['pnl_pct']}%, Duration: {results[0]['hold_duration_minutes']}min")
    
    return results


def group_by_date(simulations):
    """Group simulations by date"""
    
    daily_data = defaultdict(lambda: {'opened': [], 'closed': []})
    
    for sim in simulations:
        # Entry date
        entry_date = sim['entry_time'].date()
        daily_data[entry_date]['opened'].append(sim)
        
        # Exit date
        exit_time = sim['entry_time'] + timedelta(minutes=sim['hold_duration_minutes'])
        exit_date = exit_time.date()
        daily_data[exit_date]['closed'].append(sim)
    
    return daily_data


def simulate_daily_trading(daily_data, tracker):
    """Simulate daily trading with capital management"""
    
    daily_results = []
    
    # Sort dates
    dates = sorted(daily_data.keys())
    
    for date in dates:
        data = daily_data[date]
        
        opened_count = 0
        closed_count = 0
        total_pnl = 0
        total_fees = 0
        capital_added = 0
        
        # IMPORTANT: Process opens BEFORE closes
        # This ensures positions opened on previous days can be closed today
        
        # Open new positions
        for sim in data['opened']:
            # Add capital if needed
            if tracker.need_capital():
                added = tracker.add_capital()
                capital_added += added
            
            # Try to open position
            if tracker.can_open_position():
                tracker.open_position(sim['signal_id'])
                opened_count += 1
        
        # Close positions (may have been opened on previous days)
        for sim in data['closed']:
            if sim['signal_id'] in tracker.open_positions:
                pnl, fee = tracker.close_position(sim['signal_id'], float(sim['pnl_pct']))
                total_pnl += pnl
                total_fees += fee
                closed_count += 1
        
        # Record daily results
        daily_results.append({
            'date': date,
            'opened': opened_count,
            'closed': closed_count,
            'pnl': total_pnl,
            'fees': total_fees,
            'funding': 0,  # Simplified - could calculate based on open positions
            'balance': tracker.balance,
            'locked': tracker.locked_capital,
            'free': tracker.get_free_capital(),
            'added': capital_added
        })
    
    return daily_results


def print_daily_report(daily_results, tracker, args):
    """Print daily balance report"""
    
    print("\n" + "="*160)
    print("DAILY BALANCE REPORT")
    print(f"Position Size: ${args.position_size}, Leverage: {args.leverage}x, Initial Capital: ${args.initial_capital}")
    print("="*160)
    
    # Header
    print(f"\n{'Date':<12} {'Opened':<8} {'Closed':<8} {'PnL':<13} {'Fees':<10} {'Funding':<10} {'Balance':<15} {'Locked':<13} {'Free':<15} {'Added':<12}")
    print("-"*160)
    
    # Daily rows
    for result in daily_results:
        date_str = result['date'].strftime('%Y-%m-%d')
        pnl_str = f"$ {result['pnl']:>8.2f}"
        fees_str = f"$ {result['fees']:>6.2f}"
        funding_str = f"$ {result['funding']:>6.2f}"
        balance_str = f"$ {result['balance']:>10,.2f}"
        locked_str = f"$ {result['locked']:>8,.2f}"
        free_str = f"$ {result['free']:>10,.2f}"
        added_str = f"$ {result['added']:>8,.2f}" if result['added'] > 0 else ""
        
        print(f"{date_str:<12} {result['opened']:<8} {result['closed']:<8} {pnl_str:<13} {fees_str:<10} {funding_str:<10} {balance_str:<15} {locked_str:<13} {free_str:<15} {added_str:<12}")
    
    # Summary
    print("\n" + "="*160)
    print("FINAL SUMMARY")
    print("="*160)
    
    total_pnl = sum(r['pnl'] for r in daily_results)
    total_fees = sum(r['fees'] for r in daily_results)
    net_pnl = total_pnl - total_fees
    roi = (tracker.balance - tracker.total_invested) / tracker.total_invested * 100
    
    # Find best/worst days
    best_day = max(daily_results, key=lambda x: x['pnl'])
    worst_day = min(daily_results, key=lambda x: x['pnl'])
    
    print(f"Total Invested:    $ {tracker.total_invested:>10,.2f}")
    print(f"Final Balance:     $ {tracker.balance:>10,.2f}")
    print(f"Total PnL:         $ {total_pnl:>10,.2f}")
    print(f"Total Fees:        $ {total_fees:>10,.2f}")
    print(f"Net PnL:           $ {net_pnl:>10,.2f}")
    print(f"Total ROI:         {roi:>10.2f}%")
    print(f"Daily Avg PnL:     $ {total_pnl/len(daily_results):>10,.2f}")
    print(f"Best Day:          {best_day['date'].strftime('%Y-%m-%d')} (${best_day['pnl']:>8.2f})")
    print(f"Worst Day:         {worst_day['date'].strftime('%Y-%m-%d')} (${worst_day['pnl']:>8.2f})")
    print("="*160)


def main():
    parser = argparse.ArgumentParser(description='Daily balance report')
    parser.add_argument('--position-size', type=float, default=100,
                       help='Position size in USD (default: 100)')
    parser.add_argument('--leverage', type=int, default=10,
                       help='Leverage (default: 10)')
    parser.add_argument('--initial-capital', type=float, default=200,
                       help='Initial capital (default: 200)')
    parser.add_argument('--max-positions', type=int, default=10,
                       help='Max concurrent positions (default: 10)')
    parser.add_argument('--days', type=int, default=30,
                       help='Analysis period in days (default: 30)')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("DAILY BALANCE REPORT")
    logger.info("="*120)
    logger.info(f"Position Size: ${args.position_size}")
    logger.info(f"Leverage: {args.leverage}x")
    logger.info(f"Initial Capital: ${args.initial_capital}")
    logger.info(f"Max Positions: {args.max_positions}")
    logger.info(f"Period: {args.days} days\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load simulations
    logger.info("Loading simulations...")
    simulations = load_all_simulations(db, args.days)
    logger.info(f"Found {len(simulations)} simulations\n")
    
    if not simulations:
        logger.warning("No simulations found. Run the pipeline first.")
        db.close()
        return
    
    # Group by date
    logger.info("Grouping by date...")
    daily_data = group_by_date(simulations)
    logger.info(f"Found {len(daily_data)} trading days\n")
    
    # Initialize tracker
    tracker = CapitalTracker(
        args.initial_capital,
        args.position_size,
        args.leverage,
        args.max_positions
    )
    
    # Simulate daily trading
    logger.info("Simulating daily trading...")
    daily_results = simulate_daily_trading(daily_data, tracker)
    
    # Print report
    print_daily_report(daily_results, tracker, args)
    
    # Close connection
    db.close()
    logger.info("\n✅ Report complete!")


if __name__ == "__main__":
    main()
