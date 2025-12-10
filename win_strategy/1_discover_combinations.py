#!/usr/bin/env python3
"""
Step 1: Discover Winning Combinations
Analyzes web.scoring_history_results_v2 to find high win-rate combinations

Usage:
    python3 1_discover_combinations.py --days 30 --min-win-rate 60 --rebuild
    python3 1_discover_combinations.py --days 14 --min-signals 10 --min-win-rate 55
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import argparse
from datetime import datetime
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def discover_pattern_combinations(db: DatabaseHelper, days: int, min_signals: int, min_win_rate: float):
    """
    Find winning pattern + indicator + regime combinations
    
    Args:
        db: Database helper
        days: Number of days to analyze
        min_signals: Minimum number of signals required
        min_win_rate: Minimum win rate percentage
    
    Returns:
        List of combinations with statistics
    """
    
    logger.info(f"Analyzing last {days} days for combinations...")
    logger.info(f"Filters: min_signals={min_signals}, min_win_rate={min_win_rate}%")
    
    query = f"""
        WITH winning_signals AS (
            SELECT 
                shr.scoring_history_id,
                shr.signal_type,
                shr.is_win,
                shr.market_regime,
                sh.timestamp
            FROM web.scoring_history_results_v2 shr
            JOIN fas_v2.scoring_history sh ON sh.id = shr.scoring_history_id
            WHERE sh.timestamp >= NOW() - INTERVAL '{days} days'
        ),
        signal_patterns_agg AS (
            SELECT 
                ws.scoring_history_id,
                ARRAY_AGG(DISTINCT sp.pattern_type || '_' || sp.timeframe 
                    ORDER BY sp.pattern_type || '_' || sp.timeframe) as patterns
            FROM winning_signals ws
            JOIN fas_v2.sh_patterns shp ON shp.scoring_history_id = ws.scoring_history_id
            JOIN fas_v2.signal_patterns sp ON sp.id = shp.signal_patterns_id
            GROUP BY ws.scoring_history_id
        ),
        signal_indicators_agg AS (
            SELECT 
                ws.scoring_history_id,
                ARRAY_AGG(DISTINCT
                    CASE 
                        WHEN ind.rsi < 30 THEN 'RSI_LOW_' || shi.indicators_timeframe
                        WHEN ind.rsi > 70 THEN 'RSI_HIGH_' || shi.indicators_timeframe
                        WHEN ind.volume_zscore > 2 THEN 'VOL_HIGH_' || shi.indicators_timeframe
                        WHEN ind.volume_zscore < -2 THEN 'VOL_LOW_' || shi.indicators_timeframe
                        WHEN ind.oi_delta_pct > 5 THEN 'OI_UP_' || shi.indicators_timeframe
                        WHEN ind.oi_delta_pct < -5 THEN 'OI_DOWN_' || shi.indicators_timeframe
                        WHEN ind.buy_ratio > 0.6 THEN 'BUY_PRESSURE_' || shi.indicators_timeframe
                        WHEN ind.buy_ratio < 0.4 THEN 'SELL_PRESSURE_' || shi.indicators_timeframe
                    END
                ) FILTER (WHERE 
                    ind.rsi IS NOT NULL OR 
                    ind.volume_zscore IS NOT NULL OR
                    ind.oi_delta_pct IS NOT NULL OR
                    ind.buy_ratio IS NOT NULL
                ) as indicators
            FROM winning_signals ws
            JOIN fas_v2.sh_indicators shi ON shi.scoring_history_id = ws.scoring_history_id
            JOIN fas_v2.indicators ind ON (
                ind.trading_pair_id = shi.indicators_trading_pair_id
                AND ind.timestamp = shi.indicators_timestamp
                AND ind.timeframe = shi.indicators_timeframe
            )
            GROUP BY ws.scoring_history_id
        ),
        combined_features AS (
            SELECT 
                ws.scoring_history_id,
                ws.signal_type,
                ws.is_win,
                ws.market_regime,
                spa.patterns,
                sia.indicators
            FROM winning_signals ws
            JOIN signal_patterns_agg spa ON spa.scoring_history_id = ws.scoring_history_id
            LEFT JOIN signal_indicators_agg sia ON sia.scoring_history_id = ws.scoring_history_id
            WHERE array_length(spa.patterns, 1) >= 2  -- At least 2 patterns
        )
        SELECT 
            patterns,
            indicators,
            signal_type,
            market_regime,
            COUNT(*) as total_signals,
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END) as winning_signals,
            ROUND(100.0 * SUM(CASE WHEN is_win THEN 1 ELSE 0 END) / COUNT(*), 2) as win_rate
        FROM combined_features
        GROUP BY patterns, indicators, signal_type, market_regime
        HAVING COUNT(*) >= {min_signals}
            AND ROUND(100.0 * SUM(CASE WHEN is_win THEN 1 ELSE 0 END) / COUNT(*), 2) >= {min_win_rate}
        ORDER BY win_rate DESC, total_signals DESC
    """
    
    results = db.execute_query(query)
    logger.info(f"Found {len(results)} winning combinations")
    
    return results


def save_combinations(db: DatabaseHelper, combinations: list, days: int, mode: str = 'rebuild'):
    """
    Save combinations to optimization.winning_combinations
    
    Args:
        db: Database helper
        combinations: List of combination dictionaries
        days: Discovery period in days
        mode: 'rebuild' (clear and insert) or 'append' (insert new only)
    """
    
    if mode == 'rebuild':
        logger.info("Clearing existing combinations...")
        db.execute_query("DELETE FROM optimization.winning_combinations")
        logger.info("✅ Cleared")
    
    if not combinations:
        logger.warning("No combinations to save")
        return
    
    logger.info(f"Saving {len(combinations)} combinations...")
    
    saved_count = 0
    for combo in combinations:
        # Generate combination name
        patterns_str = '_'.join([p.split('_')[0] for p in combo['patterns'][:2]])  # First 2 pattern types
        regime_str = combo['market_regime'] or 'ANY'
        name = f"{combo['signal_type']}_{regime_str}_{patterns_str}"[:255]
        
        try:
            # Insert or update
            query = """
                INSERT INTO optimization.winning_combinations 
                (combination_name, patterns, indicators, signal_type, market_regime,
                 total_signals, winning_signals, win_rate, discovery_period_days)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (combination_name) DO UPDATE SET
                    patterns = EXCLUDED.patterns,
                    indicators = EXCLUDED.indicators,
                    total_signals = EXCLUDED.total_signals,
                    winning_signals = EXCLUDED.winning_signals,
                    win_rate = EXCLUDED.win_rate,
                    discovery_period_days = EXCLUDED.discovery_period_days,
                    discovered_at = NOW()
            """
            
            db.execute_query(query, (
                name,
                combo['patterns'],
                combo['indicators'],
                combo['signal_type'],
                combo['market_regime'],
                combo['total_signals'],
                combo['winning_signals'],
                combo['win_rate'],
                days
            ))
            
            saved_count += 1
            
        except Exception as e:
            logger.error(f"Error saving combination {name}: {e}")
            continue
    
    logger.info(f"✅ Saved {saved_count}/{len(combinations)} combinations")


def print_combinations(combinations: list):
    """Print combinations summary"""
    
    if not combinations:
        print("\nNo combinations found")
        return
    
    print("\n" + "="*120)
    print("DISCOVERED WINNING COMBINATIONS")
    print("="*120)
    print(f"{'#':<4} {'Patterns':<50} {'Type':<6} {'Regime':<8} {'Signals':<8} {'Wins':<6} {'WR %':<6}")
    print("-"*120)
    
    for idx, combo in enumerate(combinations, 1):
        patterns_str = str(combo['patterns'])[:47] + "..." if len(str(combo['patterns'])) > 50 else str(combo['patterns'])
        print(f"{idx:<4} {patterns_str:<50} {combo['signal_type']:<6} {combo['market_regime'] or 'ANY':<8} "
              f"{combo['total_signals']:<8} {combo['winning_signals']:<6} {combo['win_rate']:<6.1f}")
    
    print("="*120)
    
    # Summary stats
    total_combos = len(combinations)
    avg_wr = sum(c['win_rate'] for c in combinations) / total_combos if total_combos > 0 else 0
    total_sigs = sum(c['total_signals'] for c in combinations)
    
    print(f"\n📊 SUMMARY:")
    print(f"   Total Combinations: {total_combos}")
    print(f"   Average Win Rate: {avg_wr:.1f}%")
    print(f"   Total Signals: {total_sigs}")
    print()


def main():
    parser = argparse.ArgumentParser(description='Discover winning signal combinations')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to analyze (default: 30)')
    parser.add_argument('--min-signals', type=int, default=15,
                       help='Minimum number of signals required (default: 15)')
    parser.add_argument('--min-win-rate', type=float, default=60.0,
                       help='Minimum win rate percentage (default: 60.0)')
    parser.add_argument('--rebuild', action='store_true',
                       help='Clear existing combinations before saving')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("DISCOVER WINNING COMBINATIONS")
    logger.info("="*120)
    logger.info(f"Period: {args.days} days")
    logger.info(f"Min signals: {args.min_signals}")
    logger.info(f"Min win rate: {args.min_win_rate}%")
    logger.info(f"Mode: {'REBUILD' if args.rebuild else 'APPEND'}\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Discover combinations
    logger.info("Step 1: Discovering combinations...")
    combinations = discover_pattern_combinations(
        db, args.days, args.min_signals, args.min_win_rate
    )
    
    # Print results
    print_combinations(combinations)
    
    # Save to database
    logger.info("Step 2: Saving to database...")
    mode = 'rebuild' if args.rebuild else 'append'
    save_combinations(db, combinations, args.days, mode)
    
    # Close connection
    db.close()
    logger.info("\n✅ Discovery complete!")


if __name__ == "__main__":
    main()
