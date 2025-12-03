#!/usr/bin/env python3
"""
Fetch 1-Minute Candles from Binance
Step 2 of optimization pipeline
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime, timedelta
from optimization.utils.db_helper import DatabaseHelper
from optimization.utils.binance_client import BinanceClient
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_signals_to_fetch(db: DatabaseHelper):
    """Load signals that need candles"""
    query = """
        SELECT 
            id,
            signal_id,
            pair_symbol,
            entry_time
        FROM optimization.selected_signals
        WHERE NOT EXISTS (
            SELECT 1 
            FROM optimization.candles_1m c
            WHERE c.pair_symbol = optimization.selected_signals.pair_symbol
                AND c.open_time >= EXTRACT(EPOCH FROM optimization.selected_signals.entry_time) * 1000
                AND c.open_time < EXTRACT(EPOCH FROM optimization.selected_signals.entry_time + INTERVAL '24 hours') * 1000
            LIMIT 1
        )
        ORDER BY entry_time DESC
    """
    
    return db.execute_query(query)


def fetch_candles_for_signal(binance: BinanceClient, signal):
    """Fetch 24 hours of 1m candles for a signal"""
    entry_time = signal['entry_time']
    start_time_ms = int(entry_time.timestamp() * 1000)
    end_time_ms = start_time_ms + (24 * 60 * 60 * 1000)  # +24 hours
    
    try:
        klines = binance.get_klines(
            symbol=signal['pair_symbol'],
            interval='1m',
            start_time=start_time_ms,
            end_time=end_time_ms,
            limit=1500  # Should be enough for 1440 candles
        )
        
        # Parse candles
        candles = [binance.parse_kline(k) for k in klines]
        
        return candles
        
    except Exception as e:
        logger.error(f"Failed to fetch candles for {signal['pair_symbol']}: {e}")
        return []


def main():
    logger.info("=" * 100)
    logger.info("BINANCE 1-MINUTE CANDLE FETCHER")
    logger.info("=" * 100)
    logger.info("")
    
    # Connect to database
    logger.info("Step 1: Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Load signals
    logger.info("\nStep 2: Loading signals that need candles...")
    signals = load_signals_to_fetch(db)
    
    if not signals:
        logger.info("✅ All signals already have candles!")
        db.close()
        return
    
    logger.info(f"Found {len(signals)} signals needing candles")
    
    # Initialize Binance client
    logger.info("\nStep 3: Initializing Binance client...")
    binance = BinanceClient()
    
    # Fetch candles
    logger.info("\nStep 4: Fetching candles from Binance...")
    logger.info(f"Rate limit: 480 requests/minute (20% of max)")
    logger.info(f"Estimated time: {len(signals) * 0.125 / 60:.1f} minutes\n")
    
    total_candles = 0
    failed_signals = []
    
    for signal in tqdm(signals, desc="Fetching candles"):
        candles = fetch_candles_for_signal(binance, signal)
        
        if not candles:
            failed_signals.append(signal)
            continue
        
        # Prepare for bulk insert
        insert_data = []
        for candle in candles:
            insert_data.append((
                signal['pair_symbol'],
                candle['open_time'],
                candle['open_price'],
                candle['high_price'],
                candle['low_price'],
                candle['close_price'],
                candle['volume'],
                candle['close_time'],
                candle.get('quote_volume'),
                candle.get('trades_count')
            ))
        
        # Bulk insert
        try:
            count = db.bulk_insert(
                'optimization.candles_1m',
                ['pair_symbol', 'open_time', 'open_price', 'high_price', 'low_price',
                 'close_price', 'volume', 'close_time', 'quote_volume', 'trades_count'],
                insert_data
            )
            total_candles += count
        except Exception as e:
            logger.error(f"Error inserting candles for {signal['pair_symbol']}: {e}")
            failed_signals.append(signal)
    
    logger.info("\n" + "=" * 100)
    logger.info("SUMMARY:")
    logger.info("=" * 100)
    logger.info(f"✅ Successfully fetched: {len(signals) - len(failed_signals)}/{len(signals)} signals")
    logger.info(f"📊 Total candles inserted: {total_candles:,}")
    
    if failed_signals:
        logger.warning(f"❌ Failed signals: {len(failed_signals)}")
        for sig in failed_signals[:10]:  # Show first 10
            logger.warning(f"   - {sig['pair_symbol']} @ {sig['entry_time']}")
    
    logger.info("=" * 100)
    
    db.close()


if __name__ == "__main__":
    main()
