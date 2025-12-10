#!/usr/bin/env python3
"""
Step 6: Generate Analysis Report
Creates comprehensive markdown report with results

Usage:
    python3 6_generate_report.py
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


def load_results(db: DatabaseHelper):
    """Load all combinations with their best parameters"""
    
    query = """
        SELECT 
            wc.id,
            wc.combination_name,
            wc.patterns,
            wc.indicators,
            wc.signal_type,
            wc.market_regime,
            wc.win_rate as discovery_win_rate,
            wc.total_signals as discovery_signals,
            cbp.sl_pct,
            cbp.ts_activation_pct,
            cbp.ts_callback_pct,
            cbp.total_signals,
            cbp.winning_trades,
            cbp.losing_trades,
            cbp.win_rate,
            cbp.total_pnl_pct,
            cbp.avg_pnl_pct,
            cbp.avg_win_pct,
            cbp.avg_loss_pct,
            cbp.max_profit_pct,
            cbp.max_drawdown_pct,
            cbp.avg_hold_minutes,
            cbp.profit_factor,
            cbp.sharpe_ratio
        FROM optimization.winning_combinations wc
        JOIN optimization.combination_best_parameters cbp ON cbp.combination_id = wc.id
        WHERE wc.is_active = true
        ORDER BY cbp.total_pnl_pct DESC
    """
    
    return db.execute_query(query)


def generate_report(results: list, output_file: str):
    """Generate markdown report"""
    
    report = []
    
    # Header
    report.append("# Winning Combinations - Optimization Report")
    report.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"\n**Total Combinations:** {len(results)}")
    report.append("\n---\n")
    
    # Summary
    report.append("## 📊 Summary\n")
    
    if results:
        avg_wr = sum(r['win_rate'] for r in results) / len(results)
        avg_pnl = sum(r['total_pnl_pct'] for r in results) / len(results)
        total_signals = sum(r['total_signals'] for r in results)
        
        report.append(f"- **Average Win Rate:** {avg_wr:.1f}%")
        report.append(f"- **Average Total PnL:** {avg_pnl:.1f}%")
        report.append(f"- **Total Signals Analyzed:** {total_signals:,}")
        report.append(f"- **Best Win Rate:** {max(r['win_rate'] for r in results):.1f}%")
        report.append(f"- **Best Total PnL:** {max(r['total_pnl_pct'] for r in results):.1f}%")
    
    report.append("\n---\n")
    
    # Top combinations
    report.append("## 🏆 Top Combinations\n")
    
    for idx, result in enumerate(results[:10], 1):
        report.append(f"### #{idx} - {result['combination_name']}\n")
        
        # Performance
        report.append(f"**Performance:**")
        report.append(f"- Win Rate: **{result['win_rate']:.1f}%** ({result['winning_trades']}/{result['total_signals']} trades)")
        report.append(f"- Total PnL: **{result['total_pnl_pct']:.1f}%**")
        report.append(f"- Avg PnL: {result['avg_pnl_pct']:.2f}% per trade")
        report.append(f"- Avg Win: +{result['avg_win_pct']:.2f}%")
        report.append(f"- Avg Loss: {result['avg_loss_pct']:.2f}%")
        if result['profit_factor']:
            report.append(f"- Profit Factor: {result['profit_factor']:.2f}")
        report.append("")
        
        # Parameters
        report.append(f"**Best Parameters:**")
        report.append(f"- SL: {result['sl_pct']}%")
        report.append(f"- TS Activation: {result['ts_activation_pct']}%")
        report.append(f"- TS Callback: {result['ts_callback_pct']}%")
        report.append("")
        
        # Strategy
        report.append(f"**Strategy:**")
        report.append(f"- Type: {result['signal_type']}")
        report.append(f"- Market Regime: {result['market_regime'] or 'ANY'}")
        report.append(f"- Patterns: `{result['patterns']}`")
        if result['indicators']:
            report.append(f"- Indicators: `{result['indicators']}`")
        report.append("")
        
        # Risk
        report.append(f"**Risk Metrics:**")
        report.append(f"- Max Drawdown: {result['max_drawdown_pct']:.2f}%")
        report.append(f"- Avg Hold Time: {result['avg_hold_minutes']/60:.1f} hours")
        report.append("")
        
        report.append("---\n")
    
    # Full table
    report.append("## 📈 All Combinations\n")
    report.append("| # | Name | Type | Regime | Signals | WR % | Total PnL % | SL | TS Act | TS CB |")
    report.append("|---|------|------|--------|---------|------|-------------|----|----|-------|")
    
    for idx, result in enumerate(results, 1):
        name = result['combination_name'][:30] + "..." if len(result['combination_name']) > 33 else result['combination_name']
        report.append(
            f"| {idx} | {name} | {result['signal_type']} | {result['market_regime'] or 'ANY'} | "
            f"{result['total_signals']} | {result['win_rate']:.1f} | {result['total_pnl_pct']:.1f} | "
            f"{result['sl_pct']} | {result['ts_activation_pct']} | {result['ts_callback_pct']} |"
        )
    
    report.append("\n---\n")
    
    # Recommendations
    report.append("## 💡 Trading Recommendations\n")
    
    if results:
        best = results[0]
        report.append(f"### Recommended Strategy: {best['combination_name']}\n")
        report.append(f"**Why this works:**")
        report.append(f"- Highest total PnL: {best['total_pnl_pct']:.1f}%")
        report.append(f"- Strong win rate: {best['win_rate']:.1f}%")
        report.append(f"- Tested on {best['total_signals']} signals")
        report.append("")
        report.append(f"**How to trade:**")
        report.append(f"1. Wait for {best['signal_type']} signal in {best['market_regime'] or 'ANY'} market")
        report.append(f"2. Verify patterns: {best['patterns']}")
        if best['indicators']:
            report.append(f"3. Confirm indicators: {best['indicators']}")
        report.append(f"4. Enter with SL={best['sl_pct']}%, TS activation={best['ts_activation_pct']}%, TS callback={best['ts_callback_pct']}%")
        report.append(f"5. Expected hold time: ~{best['avg_hold_minutes']/60:.1f} hours")
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(report))
    
    logger.info(f"✅ Report saved to {output_file}")


def main():
    logger.info("="*120)
    logger.info("GENERATE ANALYSIS REPORT")
    logger.info("="*120)
    
    # Connect to database
    logger.info("\nConnecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load results
    logger.info("Loading results...")
    results = load_results(db)
    logger.info(f"Found {len(results)} combinations with optimized parameters\n")
    
    if not results:
        logger.warning("No results found. Run the full pipeline first.")
        db.close()
        return
    
    # Generate report
    os.makedirs('results', exist_ok=True)
    output_file = f"results/win_strategy_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    logger.info("Generating report...")
    generate_report(results, output_file)
    
    print(f"\n✅ Report generated: {output_file}")
    print(f"   Total combinations: {len(results)}")
    print(f"   Best win rate: {max(r['win_rate'] for r in results):.1f}%")
    print(f"   Best total PnL: {max(r['total_pnl_pct'] for r in results):.1f}%")
    
    # Close connection
    db.close()
    logger.info("\n✅ Report generation complete!")


if __name__ == "__main__":
    main()
