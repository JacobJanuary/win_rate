#!/usr/bin/env python3
"""
Step 3: Fetch Candles from Binance
Downloads 1-minute candles for all signals

Usage:
    python3 3_fetch_candles.py
    python3 3_fetch_candles.py --batch-size 50
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import argparse
from datetime import datetime
from optimization.utils.db_helper import DatabaseHelper
from win_strategy.utils.candle_fetcher import CandleFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_signals_without_candles(db: DatabaseHelper, limit: int = None):
    """
    Load signals that don't have candles yet
    
    Args:
        db: Database helper
        limit: Maximum number of signals to load (None = all)
    
    Returns:
        List of signal dictionaries
    """
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    query = f"""
        SELECT 
            cs.id,
            cs.pair_symbol,
            cs.entry_time,
            cs.signal_timestamp
        FROM optimization.combination_signals cs
        LEFT JOIN optimization.combination_candles cc ON cc.signal_id = cs.id
        WHERE cc.id IS NULL
        ORDER BY cs.entry_time
        {limit_clause}
    """
    
    results = db.execute_query(query)
    logger.info(f"Found {len(results)} signals without candles")
    
    return results


def save_candles(db: DatabaseHelper, signal_id: int, pair_symbol: str, candles: list):
    """
    Save candles to database
    
    Args:
        db: Database helper
        signal_id: Signal ID
        pair_symbol: Trading pair symbol
        candles: List of candle dictionaries
    
    Returns:
        Number of candles saved
    """
    
    if not candles:
        return 0
    
    saved_count = 0
    for candle in candles:
        try:
            query = """
                INSERT INTO optimization.combination_candles 
                (signal_id, pair_symbol, open_time, open_price, high_price, 
                 low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (signal_id, open_time) DO NOTHING
            """
            
            db.execute_update(query, (
                signal_id,
                pair_symbol,
                candle['open_time'],
                candle['open_price'],
                candle['high_price'],
                candle['low_price'],
                candle['close_price'],
                candle['volume']
            ))
            
            saved_count += 1
            
        except Exception as e:
            logger.error(f"Error saving candle: {e}")
            continue
    
    return saved_count


def main():
    parser = argparse.ArgumentParser(description='Fetch candles from Binance')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of signals to process (default: 100, 0 = all)')
    parser.add_argument('--candles-per-signal', type=int, default=1440,
                       help='Number of candles per signal (default: 1440 = 24h)')
    args = parser.parse_args()
    
    logger.info("="*120)
    logger.info("FETCH CANDLES FROM BINANCE")
    logger.info("="*120)
    logger.info(f"Batch size: {args.batch_size if args.batch_size > 0 else 'ALL'}")
    logger.info(f"Candles per signal: {args.candles_per_signal}\n")
    
    # Connect to database
    logger.info("Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    logger.info("✅ Connected\n")
    
    # Load signals
    logger.info("Step 1: Loading signals without candles...")
    limit = args.batch_size if args.batch_size > 0 else None
    signals = load_signals_without_candles(db, limit)
    
    if not signals:
        logger.info("No signals need candles. All done!")
        db.close()
        return
    
    print(f"\nWill fetch candles for {len(signals)} signals")
    print(f"Estimated time: {len(signals) * 0.15:.1f} minutes (at 480 req/min)\n")
    
    # Initialize fetcher
    logger.info("Step 2: Initializing Binance client...")
    fetcher = CandleFetcher(requests_per_minute=250)  # Conservative rate limit
    logger.info("✅ Initialized\n")
    
    # Fetch candles
    logger.info("Step 3: Fetching candles...")
    total_candles = 0
    failed_signals = []
    
    for idx, signal in enumerate(signals, 1):
        signal_id = signal['id']
        pair_symbol = signal['pair_symbol']
        entry_time = signal['entry_time']
        
        logger.info(f"[{idx}/{len(signals)}] {pair_symbol} at {entry_time}")
        
        # Fetch candles
        candles = fetcher.fetch_for_signal(
            pair_symbol,
            entry_time,
            args.candles_per_signal
        )
        
        if candles:
            # Save to database
            saved = save_candles(db, signal_id, pair_symbol, candles)
            total_candles += saved
            logger.info(f"  ✅ Saved {saved} candles")
        else:
            logger.error(f"  ❌ Failed to fetch candles")
            failed_signals.append(signal_id)
    
    # Summary
    print("\n" + "="*120)
    print("SUMMARY")
    print("="*120)
    print(f"Signals processed: {len(signals)}")
    print(f"Successful: {len(signals) - len(failed_signals)}")
    print(f"Failed: {len(failed_signals)}")
    print(f"Total candles saved: {total_candles:,}")
    print(f"Average candles per signal: {total_candles/len(signals):.0f}")
    
    if failed_signals:
        print(f"\n⚠️  Failed signal IDs: {failed_signals[:10]}")
        if len(failed_signals) > 10:
            print(f"   ... and {len(failed_signals) - 10} more")
    
    print("="*120)
    
    # Close connection
    db.close()
    logger.info("\n✅ Candle fetching complete!")


if __name__ == "__main__":
    main()
