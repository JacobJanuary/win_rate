"""
Candle Fetcher Utility
Fetches 1-minute candles from Binance API with rate limiting
"""

import requests
import time
import logging
from typing import List, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CandleFetcher:
    """Fetch candles from Binance with rate limiting"""
    
    def __init__(self, requests_per_minute: int = 250):
        """
        Initialize candle fetcher
        
        Args:
            requests_per_minute: Max requests per minute (default: 250)
        """
        self.base_url = "https://fapi.binance.com"
        self.requests_per_minute = requests_per_minute
        self.request_delay = 60.0 / requests_per_minute  # Seconds between requests
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Enforce rate limiting"""
        now = time.time()
        time_since_last = now - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def fetch_candles(
        self, 
        symbol: str, 
        start_time: datetime, 
        limit: int = 1440
    ) -> List[Dict]:
        """
        Fetch 1-minute candles from Binance
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start_time: Start time for candles
            limit: Number of candles to fetch (max 1500, default 1440 = 24h)
        
        Returns:
            List of candle dictionaries
        """
        
        # Rate limiting
        self._rate_limit()
        
        # Convert start_time to milliseconds
        start_ms = int(start_time.timestamp() * 1000)
        
        # Build request
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'startTime': start_ms,
            'limit': min(limit, 1500)  # Binance max is 1500
        }
        
        try:
            logger.debug(f"Fetching {limit} candles for {symbol} from {start_time}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to our format
            candles = []
            for candle in data:
                candles.append({
                    'open_time': int(candle[0]),
                    'open_price': float(candle[1]),
                    'high_price': float(candle[2]),
                    'low_price': float(candle[3]),
                    'close_price': float(candle[4]),
                    'volume': float(candle[5])
                })
            
            logger.debug(f"Fetched {len(candles)} candles for {symbol}")
            return candles
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching candles for {symbol}: {e}")
            return []
    
    def fetch_for_signal(
        self,
        pair_symbol: str,
        entry_time: datetime,
        candles_count: int = 1440
    ) -> List[Dict]:
        """
        Fetch candles for a specific signal
        
        Args:
            pair_symbol: Trading pair (e.g., 'BTCUSDT')
            entry_time: Signal entry time (timestamp + 18 min)
            candles_count: Number of candles (default: 1440 = 24h)
        
        Returns:
            List of candle dictionaries
        """
        
        # Fetch candles
        candles = self.fetch_candles(pair_symbol, entry_time, candles_count)
        
        if not candles:
            logger.warning(f"No candles fetched for {pair_symbol} at {entry_time}")
            return []
        
        # Validate we got enough candles
        if len(candles) < candles_count * 0.9:  # Allow 10% tolerance
            logger.warning(
                f"Only got {len(candles)}/{candles_count} candles for {pair_symbol}"
            )
        
        return candles
    
    def fetch_batch(
        self,
        signals: List[Dict],
        candles_per_signal: int = 1440
    ) -> Dict[int, List[Dict]]:
        """
        Fetch candles for multiple signals
        
        Args:
            signals: List of signal dictionaries with 'id', 'pair_symbol', 'entry_time'
            candles_per_signal: Number of candles per signal
        
        Returns:
            Dictionary mapping signal_id to list of candles
        """
        
        results = {}
        total = len(signals)
        
        for idx, signal in enumerate(signals, 1):
            signal_id = signal['id']
            pair_symbol = signal['pair_symbol']
            entry_time = signal['entry_time']
            
            logger.info(f"[{idx}/{total}] Fetching candles for {pair_symbol}...")
            
            candles = self.fetch_for_signal(
                pair_symbol, 
                entry_time, 
                candles_per_signal
            )
            
            if candles:
                results[signal_id] = candles
            else:
                logger.error(f"Failed to fetch candles for signal {signal_id}")
        
        logger.info(f"Fetched candles for {len(results)}/{total} signals")
        return results
