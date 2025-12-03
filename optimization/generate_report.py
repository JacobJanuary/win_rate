#!/usr/bin/env python3
"""
Generate Optimization Report
Step 5 of optimization pipeline
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


def generate_report():
    """Generate comprehensive optimization report"""
    
    # Load aggregated results
    df = pd.read_csv('results/aggregated_results.csv')
    
    # Generate markdown report
    report = []
    
    report.append("# Strategy Optimization Report")
    report.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"\n**Total Combinations Analyzed:** {len(df):,}")
    report.append("\n---\n")
    
    # Executive Summary
    report.append("## Executive Summary\n")
    report.append(f"- **Average Win Rate:** {df['win_rate'].mean():.2f}%")
    report.append(f"- **Average Total PnL:** {df['total_pnl_pct'].mean():.2f}%")
    report.append(f"- **Best Win Rate:** {df['win_rate'].max():.2f}%")
    report.append(f"- **Best Total PnL:** {df['total_pnl_pct'].max():.2f}%")
    report.append(f"- **Total Signals Analyzed:** {df['total_signals'].sum():,}")
    report.append("\n---\n")
    
    # Top 10 by Total PnL
    report.append("## Top-10 Strategies by Total PnL\n")
    top10_pnl = df.nlargest(10, 'total_pnl_pct')
    
    report.append("| # | Strategy | SL% | TS_Act% | TS_CB% | WR% | Total PnL% | Signals |")
    report.append("|---|----------|-----|---------|--------|-----|------------|---------|")
    
    for idx, (i, row) in enumerate(top10_pnl.iterrows(), 1):
        strategy_name = row['strategy_name'][:50]
        report.append(f"| {idx} | {strategy_name} | {row['sl_pct']:.1f} | "
                     f"{row['ts_activation_pct']:.1f} | {row['ts_callback_pct']:.1f} | "
                     f"{row['win_rate']:.1f} | {row['total_pnl_pct']:.2f} | {row['total_signals']} |")
    
    report.append("\n---\n")
    
    # Top 10 by Win Rate
    report.append("## Top-10 Strategies by Win Rate\n")
    top10_wr = df.nlargest(10, 'win_rate')
    
    report.append("| # | Strategy | SL% | TS_Act% | TS_CB% | WR% | Total PnL% | Signals |")
    report.append("|---|----------|-----|---------|--------|-----|------------|---------|")
    
    for idx, (i, row) in enumerate(top10_wr.iterrows(), 1):
        strategy_name = row['strategy_name'][:50]
        report.append(f"| {idx} | {strategy_name} | {row['sl_pct']:.1f} | "
                     f"{row['ts_activation_pct']:.1f} | {row['ts_callback_pct']:.1f} | "
                     f"{row['win_rate']:.1f} | {row['total_pnl_pct']:.2f} | {row['total_signals']} |")
    
    report.append("\n---\n")
    
    # Parameter Analysis
    report.append("## Optimal Parameters by Direction\n")
    
    # LONG strategies
    long_df = df[df['signal_type'] == 'LONG']
    if len(long_df) > 0:
        best_long = long_df.nlargest(1, 'total_pnl_pct').iloc[0]
        report.append(f"\n### LONG Strategies\n")
        report.append(f"**Best Parameters:**")
        report.append(f"- SL: {best_long['sl_pct']:.1f}%")
        report.append(f"- TS Activation: {best_long['ts_activation_pct']:.1f}%")
        report.append(f"- TS Callback: {best_long['ts_callback_pct']:.1f}%")
        report.append(f"- Win Rate: {best_long['win_rate']:.2f}%")
        report.append(f"- Total PnL: {best_long['total_pnl_pct']:.2f}%")
    
    # SHORT strategies
    short_df = df[df['signal_type'] == 'SHORT']
    if len(short_df) > 0:
        best_short = short_df.nlargest(1, 'total_pnl_pct').iloc[0]
        report.append(f"\n### SHORT Strategies\n")
        report.append(f"**Best Parameters:**")
        report.append(f"- SL: {best_short['sl_pct']:.1f}%")
        report.append(f"- TS Activation: {best_short['ts_activation_pct']:.1f}%")
        report.append(f"- TS Callback: {best_short['ts_callback_pct']:.1f}%")
        report.append(f"- Win Rate: {best_short['win_rate']:.2f}%")
        report.append(f"- Total PnL: {best_short['total_pnl_pct']:.2f}%")
    
    report.append("\n---\n")
    
    # Implementation Recommendations
    report.append("## Implementation Recommendations\n")
    
    best_overall = df.nlargest(1, 'total_pnl_pct').iloc[0]
    
    report.append(f"\n**Recommended Parameters for Production:**\n")
    report.append(f"```python")
    report.append(f"OPTIMAL_PARAMS = {{")
    report.append(f"    'sl_pct': {best_overall['sl_pct']:.1f},")
    report.append(f"    'ts_activation_pct': {best_overall['ts_activation_pct']:.1f},")
    report.append(f"    'ts_callback_pct': {best_overall['ts_callback_pct']:.1f}")
    report.append(f"}}")
    report.append(f"```")
    
    report.append(f"\n**Expected Performance:**")
    report.append(f"- Win Rate: ~{best_overall['win_rate']:.1f}%")
    report.append(f"- Avg PnL per trade: ~{best_overall['avg_pnl_pct']:.2f}%")
    report.append(f"- Max Drawdown: ~{best_overall['max_drawdown_pct']:.2f}%")
    
    report.append("\n---\n")
    report.append("*End of Report*")
    
    # Write to file
    report_text = '\n'.join(report)
    
    with open('results/optimization_report.md', 'w') as f:
        f.write(report_text)
    
    logger.info("✅ Report generated: results/optimization_report.md")
    
    return report_text


def main():
    logger.info("=" * 100)
    logger.info("REPORT GENERATION")
    logger.info("=" * 100)
    logger.info("")
    
    logger.info("Generating optimization report...")
    report = generate_report()
    
    # Print report to console
    print("\n" + report)
    
    logger.info("\n" + "=" * 100)
    logger.info("✅ REPORT COMPLETE")
    logger.info("=" * 100)


if __name__ == "__main__":
    main()
