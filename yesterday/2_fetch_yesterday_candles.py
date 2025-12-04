#!/usr/bin/env python3
"""
Step 2: Fetch Yesterday's Candles from Binance
Download 1-minute candles for all selected yesterday signals
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from optimization.utils.db_helper import DatabaseHelper
from optimization.utils.binance_client import BinanceClient
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_signals_needing_candles(db: DatabaseHelper):
    """Load signals that don't have candles yet"""
    
    query = """
        SELECT 
            signal_id,
            pair_symbol,
            entry_time
        FROM optimization.yesterday_signals
        WHERE NOT EXISTS (
            SELECT 1 
            FROM optimization.yesterday_candles c
            WHERE c.pair_symbol = optimization.yesterday_signals.pair_symbol
                AND c.open_time >= EXTRACT(EPOCH FROM optimization.yesterday_signals.entry_time) * 1000
                AND c.open_time < EXTRACT(EPOCH FROM optimization.yesterday_signals.entry_time + INTERVAL '24 hours') * 1000
            LIMIT 1
        )
        ORDER BY entry_time
    """
    
    return db.execute_query(query)


def fetch_candles_for_signal(binance: BinanceClient, signal):
    """Fetch 24 hours of 1m candles starting from entry_time"""
    
    entry_time = signal['entry_time']
    start_time_ms = int(entry_time.timestamp() * 1000)
    end_time_ms = start_time_ms + (24 * 60 * 60 * 1000)  # +24 hours
    
    try:
        klines = binance.get_klines(
            symbol=signal['pair_symbol'],
            interval='1m',
            start_time=start_time_ms,
            end_time=end_time_ms,
            limit=1500
        )
        
        candles = [binance.parse_kline(k) for k in klines]
        return candles
        
    except Exception as e:
        logger.error(f"Failed to fetch candles for {signal['pair_symbol']}: {e}")
        return []


def insert_candles(db: DatabaseHelper, pair_symbol, candles):
    """Insert candles into yesterday_candles table"""
    
    if not candles:
        return 0
    
    insert_data = []
    for candle in candles:
        insert_data.append((
            pair_symbol,
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
    
    # Use specialized method for candles
    inserted = db.bulk_insert_candles(
        'optimization.yesterday_candles',
        ['pair_symbol', 'open_time', 'open_price', 'high_price', 'low_price',
         'close_price', 'volume', 'close_time', 'quote_volume', 'trades_count'],
        insert_data
    )
    
    return inserted


def main():
    logger.info("=" * 100)
    logger.info("YESTERDAY CANDLES FETCHER")
    logger.info("=" * 100)
    logger.info("")
    
    # Connect
    logger.info("Step 1: Connecting to database...")
    db = DatabaseHelper()
    db.connect()
    
    # Load signals
    logger.info("\nStep 2: Loading signals needing candles...")
    signals = load_signals_needing_candles(db)
    
    if not signals:
        logger.info("✅ All signals already have candles!")
        db.close()
        return
    
    logger.info(f"Found {len(signals)} signals needing candles")
    
    # Init Binance client
    logger.info("\nStep 3: Initializing Binance client...")
    binance = BinanceClient()
    
    # Fetch candles
    logger.info("\nStep 4: Fetching candles from Binance...")
    logger.info(f"Rate limit: 200 requests/minute")
    logger.info(f"Estimated time: {len(signals) * 0.3 / 60:.1f} minutes\n")
    
    total_candles = 0
    failed = []
    
    for signal in tqdm(signals, desc="Fetching candles"):
        candles = fetch_candles_for_signal(binance, signal)
        
        if not candles:
            failed.append(signal)
            continue
        
        try:
            inserted = insert_candles(db, signal['pair_symbol'], candles)
            total_candles += inserted
        except Exception as e:
            logger.error(f"Error inserting candles for {signal['pair_symbol']}: {e}")
            if db.conn:
                db.conn.rollback()
            failed.append(signal)
    
    # Summary
    logger.info("\n" + "=" * 100)
    logger.info("SUMMARY:")
    logger.info("=" * 100)
    logger.info(f"✅ Successfully fetched: {len(signals) - len(failed)}/{len(signals)} signals")
    logger.info(f"📊 Total candles inserted: {total_candles:,}")
    
    if failed:
        logger.warning(f"❌ Failed signals: {len(failed)}")
        for sig in failed[:10]:
            logger.warning(f"   - {sig['pair_symbol']}")
    
    logger.info("=" * 100)
    
    db.close()
    logger.info("\n✅ Complete!")


if __name__ == "__main__":
    main()
