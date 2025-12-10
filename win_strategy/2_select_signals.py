#!/usr/bin/env python3
"""
Step 2: Select Signals Matching Combinations
Finds all signals that match discovered winning combinations

Usage:
    python3 2_select_signals.py
    python3 2_select_signals.py --days 30
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import argparse
from datetime import datetime, timedelta
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_active_combinations(db: DatabaseHelper):
    """Load all active combinations from database"""
    
    query = """
        SELECT 
            id,
            combination_name,
            patterns,
            indicators,
            signal_type,
            market_regime,
            win_rate
        FROM optimization.winning_combinations
        WHERE is_active = true
        ORDER BY win_rate DESC
    """
    
    results = db.execute_query(query)
    logger.info(f"Loaded {len(results)} active combinations")
    
    return results


def find_matching_signals(db: DatabaseHelper, combination: dict, days: int):
    """
    Find all signals matching a specific combination
    
    Args:
        db: Database helper
        combination: Combination dictionary with patterns, indicators, etc.
        days: Number of days to look back
    
    Returns:
        List of matching signals
    """
    
    combo_id = combination['id']
    patterns = combination['patterns']
    indicators = combination['indicators']
    signal_type = combination['signal_type']
    market_regime = combination['market_regime']
    
    # Build query to find matching signals
    query = f"""
        WITH time_filtered_signals AS (
            SELECT 
                sh.id as scoring_history_id,
                sh.pair_symbol,
                sh.timestamp as signal_timestamp,
                sh.timestamp + INTERVAL '18 minutes' as entry_time,
                sh.total_score,
                sh.pattern_score,
                sh.indicator_score,
                shr.signal_type,
                shr.market_regime
            FROM fas_v2.scoring_history sh
            JOIN web.scoring_history_results_v2 shr ON shr.scoring_history_id = sh.id
            WHERE sh.timestamp >= NOW() - INTERVAL '{days} days'
                AND sh.timestamp < NOW() - INTERVAL '1 day'
                AND shr.signal_type = %s
                AND (shr.market_regime = %s OR %s IS NULL)
        ),
        signal_patterns_agg AS (
            SELECT 
                tfs.scoring_history_id,
                ARRAY_AGG(DISTINCT sp.pattern_type || '_' || sp.timeframe 
                    ORDER BY sp.pattern_type || '_' || sp.timeframe) as patterns
            FROM time_filtered_signals tfs
            JOIN fas_v2.sh_patterns shp ON shp.scoring_history_id = tfs.scoring_history_id
            JOIN fas_v2.signal_patterns sp ON sp.id = shp.signal_patterns_id
            GROUP BY tfs.scoring_history_id
        ),
        signal_indicators_agg AS (
            SELECT 
                tfs.scoring_history_id,
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
            FROM time_filtered_signals tfs
            JOIN fas_v2.sh_indicators shi ON shi.scoring_history_id = tfs.scoring_history_id
            JOIN fas_v2.indicators ind ON (
                ind.trading_pair_id = shi.indicators_trading_pair_id
                AND ind.timestamp = shi.indicators_timestamp
                AND ind.timeframe = shi.indicators_timeframe
            )
            GROUP BY tfs.scoring_history_id
        )
        SELECT 
            tfs.scoring_history_id,
            tfs.pair_symbol,
            tfs.signal_timestamp,
            tfs.entry_time,
            tfs.signal_type,
            tfs.market_regime,
            tfs.total_score,
            tfs.pattern_score,
            tfs.indicator_score
        FROM time_filtered_signals tfs
        JOIN signal_patterns_agg spa ON spa.scoring_history_id = tfs.scoring_history_id
        LEFT JOIN signal_indicators_agg sia ON sia.scoring_history_id = tfs.scoring_history_id
        WHERE spa.patterns = %s
            AND (sia.indicators = %s OR (%s IS NULL AND sia.indicators IS NULL))
    """
    
    results = db.execute_query(query, (
        signal_type,
        market_regime,
        market_regime,
        patterns,
        indicators,
        indicators
    ))
    
    return results


def save_signals(db: DatabaseHelper, combination_id: int, signals: list):
    """
    Save signals to combination_signals table
    
    Args:
        db: Database helper
        combination_id: ID of the combination
        signals: List of signal dictionaries
    """
    
    if not signals:
        return 0
    
    saved_count = 0
    for signal in signals:
        try:
            query = """
                INSERT INTO optimization.combination_signals 
                (combination_id, scoring_history_id, pair_symbol, signal_timestamp, 
                 entry_time, signal_type, market_regime, total_score, pattern_score, indicator_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (combination_id, scoring_history_id) DO NOTHING
            """
            
            db.execute_query(query, (
                combination_id,
                signal['scoring_history_id'],
                signal['pair_symbol'],
                signal['signal_timestamp'],
                signal['entry_time'],
                signal['signal_type'],
                signal['market_regime'],
                signal['total_score'],
                signal['pattern_score'],
                signal['indicator_score']
            ))
            
            saved_count += 1
            
        except Exception as e:
            logger.error(f"Error saving signal {signal['scoring_history_id']}: {e}")
            continue
    
    return saved_count


def main():
    parser = argparse.ArgumentParser(description='Select signals matching winning combinations')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to look back (default: 30)')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("SELECT SIGNALS FOR WINNING COMBINATIONS")
    logger.info("="*120)
    logger.info(f"Period: Last {args.days} days\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load combinations
    logger.info("Step 1: Loading active combinations...")
    combinations = load_active_combinations(db)
    
    if not combinations:
        logger.warning("No active combinations found. Run 1_discover_combinations.py first.")
        db.close()
        return
    
    print(f"\nFound {len(combinations)} combinations:")
    for idx, combo in enumerate(combinations, 1):
        print(f"  {idx}. {combo['combination_name']} (WR: {combo['win_rate']:.1f}%)")
    print()
    
    # Process each combination
    logger.info("Step 2: Finding matching signals...")
    total_signals = 0
    
    for idx, combo in enumerate(combinations, 1):
        logger.info(f"\n[{idx}/{len(combinations)}] Processing: {combo['combination_name']}")
        
        # Find matching signals
        signals = find_matching_signals(db, combo, args.days)
        logger.info(f"  Found {len(signals)} matching signals")
        
        # Save to database
        if signals:
            saved = save_signals(db, combo['id'], signals)
            logger.info(f"  Saved {saved} signals")
            total_signals += saved
        else:
            logger.info(f"  No signals to save")
    
    # Summary
    print("\n" + "="*120)
    print("SUMMARY")
    print("="*120)
    print(f"Combinations processed: {len(combinations)}")
    print(f"Total signals saved: {total_signals}")
    print(f"Average signals per combination: {total_signals/len(combinations):.1f}")
    print("="*120)
    
    # Close connection
    db.close()
    logger.info("\n✅ Signal selection complete!")


if __name__ == "__main__":
    main()
