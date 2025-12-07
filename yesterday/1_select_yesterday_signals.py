#!/usr/bin/env python3
"""
Step 1: Select Yesterday's Signals
Extract signals from 24-48 hours ago that match top strategies (total_pnl > 180%)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime, timedelta
from optimization.utils.db_helper import DatabaseHelper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_yesterday_signals(db: DatabaseHelper, min_total_pnl: float = 180):
    """
    Get signals from 24-48 hours ago for top strategies
    
    For each signal, includes the best optimized parameters from best_parameters table
    """
    
    # Calculate time windows (tz-aware for DB, will convert later)
    now = datetime.now()
    start_time = now - timedelta(hours=48)
    end_time = now - timedelta(hours=24)
    
    logger.info(f"Time window: {start_time} to {end_time}")
    
    query = f"""
        WITH best_strategies AS (
            -- Get top strategies with total_pnl > threshold
            -- Extract patterns from strategy_name
            SELECT DISTINCT ON (strategy_name, signal_type, market_regime)
                strategy_name,
                signal_type,
                market_regime,
                sl_pct,
                ts_activation_pct,
                ts_callback_pct,
                total_pnl_pct,
                -- Extract patterns array from strategy_name format: "['PAT1', 'PAT2']|TYPE|REGIME"
                string_to_array(
                    regexp_replace(
                        split_part(strategy_name, '|', 1),  -- Get ['PAT1', 'PAT2'] part
                        '[\[\]'']',  -- Remove brackets and quotes
                        '',
                        'g'
                    ),
                    ', '  -- Split by comma-space
                ) as required_patterns
            FROM optimization.best_parameters
            WHERE total_pnl_pct > {min_total_pnl}
            ORDER BY
                strategy_name,
                signal_type,
                market_regime,
                total_pnl_pct DESC
        ),
        yesterday_window AS (
            -- Get signals from yesterday (24-48hrs ago)
            SELECT
                sh.id as signal_id,
                sh.pair_symbol,
                sh.timestamp as signal_timestamp,
                sh.total_score,
                
                -- Aggregate patterns
                ARRAY_AGG(DISTINCT sp.pattern_type || '_' || sp.timeframe) as patterns,
                
                -- Get market regime
                mr.regime as market_regime
                
            FROM fas_v2.scoring_history sh
            JOIN fas_v2.sh_patterns shp ON shp.scoring_history_id = sh.id
            JOIN fas_v2.signal_patterns sp ON sp.id = shp.signal_patterns_id
            INNER JOIN public.trading_pairs tp ON tp.pair_symbol = sh.pair_symbol
            LEFT JOIN fas_v2.sh_regime shr_regime ON shr_regime.scoring_history_id = sh.id
            LEFT JOIN fas_v2.market_regime mr ON mr.id = shr_regime.signal_regime_id
            WHERE sh.timestamp >= %(start_time)s
                AND sh.timestamp < %(end_time)s
                AND tp.exchange_id = 1
                AND tp.contract_type_id = 1
                AND tp.is_active = true
            GROUP BY sh.id, sh.pair_symbol, sh.timestamp, sh.total_score, mr.regime
            HAVING COUNT(DISTINCT sp.id) >= 2  -- Multi-pattern only
        )
        SELECT
            yw.signal_id,
            yw.pair_symbol,
            yw.signal_timestamp,
            yw.signal_timestamp + INTERVAL '17 minutes' as entry_time,
            bs.signal_type,  -- From best_parameters, not results_v2
            yw.patterns,
            yw.market_regime,
            yw.total_score,
            bs.strategy_name,
            bs.sl_pct,
            bs.ts_activation_pct,
            bs.ts_callback_pct
        FROM yesterday_window yw
        JOIN best_strategies bs ON (
            bs.market_regime = yw.market_regime
            AND yw.patterns @> bs.required_patterns  -- EXACT pattern match
        )
        WHERE yw.market_regime IS NOT NULL
        ORDER BY yw.signal_timestamp
    """
    
    params = {
        'start_time': start_time,
        'end_time': end_time
    }
    
    return db.execute_query(query, params)


def save_signals(db: DatabaseHelper, signals):
    """Save signals to yesterday_signals table"""
    
    if not signals:
        logger.warning("No signals to save")
        return 0
    
    # Clear existing data
    logger.info("Clearing existing yesterday signals...")
    deleted = db.execute_update("DELETE FROM optimization.yesterday_signals")
    logger.info(f"Deleted {deleted} existing signals")
    
    # Prepare insert data
    insert_data = []
    for sig in signals:
        insert_data.append((
            sig['signal_id'],
            sig['pair_symbol'],
            sig['signal_timestamp'],
            sig['entry_time'],
            sig['signal_type'],
            sig['patterns'],  # PostgreSQL array
            sig['market_regime'],
            float(sig['total_score']) if sig['total_score'] else None,
            sig['strategy_name'],
            float(sig['sl_pct']),
            float(sig['ts_activation_pct']),
            float(sig['ts_callback_pct'])
        ))
    
    # Bulk insert
    query = """
        INSERT INTO optimization.yesterday_signals 
        (signal_id, pair_symbol, signal_timestamp, entry_time, signal_type, 
         patterns, market_regime, total_score, strategy_name,
         sl_pct, ts_activation_pct, ts_callback_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (signal_id) DO NOTHING
    """
    
    with db.conn.cursor() as cur:
        cur.executemany(query, insert_data)
        inserted = cur.rowcount
        db.conn.commit()
    
    logger.info(f"✅ Inserted {inserted} signals")
    return inserted


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Select yesterday signals for analysis')
    parser.add_argument('--min-pnl', type=float, default=180,
                       help='Minimum strategy total_pnl_pct (default: 180)')
    args = parser.parse_args()
    
    logger.info("=" * 100)
    logger.info("YESTERDAY SIGNALS SELECTION")
    logger.info("=" * 100)
    logger.info(f"Strategy filter: total_pnl > {args.min_pnl}%\n")
    
    # Connect
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Get signals
    logger.info("Fetching signals from 24-48 hours ago...")
    signals = get_yesterday_signals(db, args.min_pnl)
    
    if not signals:
        logger.warning("No signals found in time window!")
        db.close()
        return
    
    logger.info(f"Found {len(signals)} signals")
    
    # Save to DB
    logger.info("\nSaving to yesterday_signals table...")
    inserted = save_signals(db, signals)
    
    # Statistics
    logger.info("\n" + "=" * 100)
    logger.info("SUMMARY:")
    logger.info("=" * 100)
    logger.info(f"Total signals selected: {inserted}")
    
    # Count by strategy
    query = """
        SELECT 
            strategy_name,
            signal_type,
            market_regime,
            COUNT(*) as count
        FROM optimization.yesterday_signals
        GROUP BY strategy_name, signal_type, market_regime
        ORDER BY count DESC
        LIMIT 10
    """
    stats = db.execute_query(query)
    
    logger.info("\nTop-10 strategies:")
    for s in stats:
        logger.info(f"  {s['strategy_name'][:60]:60} | {s['signal_type']:5} | {s['market_regime']:8} | {s['count']:3} signals")
    
    logger.info("=" * 100)
    
    db.close()
    logger.info("\n✅ Complete!")


if __name__ == "__main__":
    main()
