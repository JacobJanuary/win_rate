"""
Binance API client for fetching historical candles
"""

import requests
import time
import logging
from typing import List, Dict
import yaml

logger = logging.getLogger(__name__)


class BinanceClient:
    """Binance Public API client with rate limiting"""
    
    def __init__(self, config_path='config/optimization_config.yaml'):
        """Initialize with config"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = self.config['binance']['base_url']
        
        # Rate limiting: 200 requests/minute (safe limit)
        self.requests_per_minute = 200
        self.request_delay = 60.0 / self.requests_per_minute  # 0.3 seconds between requests
        
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        logger.info(f"Binance client initialized: {self.requests_per_minute} req/min")
        
        self.request_count = 0
        self.last_request_time = time.time()
        self.last_minute_start = time.time()
    
    def get_klines(
        self, 
        symbol: str, 
        interval: str, 
        start_time: int, 
        end_time: int,
        limit: int = 1500
    ) -> List[List]:
        """
        Fetch klines/candlestick data from public API
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Candlestick interval (e.g., '1m')
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            limit: Number of candles to fetch (max 1500)
        
        Returns:
            List of candles [open_time, open, high, low, close, volume, ...]
        """
        # Rate limiting
        self._check_rate_limit()
        
        # Public API endpoint
        url = f"{self.base_url}/fapi/v1/klines"
        
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            klines = response.json()
            
            self.request_count += 1
            logger.debug(f"Fetched {len(klines)} candles for {symbol}")
            
            # Enforce delay between requests
            time.sleep(self.request_delay)
            
            return klines
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit exceeded
                logger.warning(f"Rate limit exceeded for {symbol}, waiting 60s...")
                time.sleep(60)
                return self.get_klines(symbol, interval, start_time, end_time, limit)
            else:
                logger.error(f"Binance API error for {symbol}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            raise
    
    def _check_rate_limit(self):
        """Check and enforce rate limits (20% of max bandwidth)"""
        current_time = time.time()
        time_since_minute_start = current_time - self.last_minute_start
        
        # Reset counter every minute
        if time_since_minute_start >= 60:
            self.request_count = 0
            self.last_minute_start = current_time
            logger.debug(f"Rate limit counter reset")
        
        # If approaching limit, wait
        if self.request_count >= self.rate_limit:
            wait_time = 60 - time_since_minute_start + 1  # Wait until next minute
            logger.info(f"Rate limit reached ({self.request_count} reqs), waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            self.request_count = 0
            self.last_minute_start = time.time()
    
    def parse_kline(self, kline: List) -> Dict:
        """
        Parse kline/candle data into dict
        
        Args:
            kline: Raw kline data from Binance
        
        Returns:
            Dict with candle fields
        """
        return {
            'open_time': int(kline[0]),
            'open_price': float(kline[1]),
            'high_price': float(kline[2]),
            'low_price': float(kline[3]),
            'close_price': float(kline[4]),
            'volume': float(kline[5]),
            'close_time': int(kline[6]),
            'quote_volume': float(kline[7]),
            'trades_count': int(kline[8])
        }
