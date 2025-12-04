"""
Database helper utilities for optimization system
"""

import os
import psycopg
from psycopg.rows import dict_row
from typing import List, Dict, Any
import logging
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env file
env_path = Path(__file__).parents[2] / '.env'
if env_path.exists():
    load_dotenv(env_path)
    logger.info(f"✅ Loaded .env from {env_path}")


class DatabaseHelper:
    """Helper for database operations"""
    
    def __init__(self):
        """Initialize with credentials from environment variables"""
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5433)),
            'dbname': os.getenv('DB_NAME', 'fox_crypto_new'),
            'user': os.getenv('DB_USER', 'elcrypto_readonly'),
            'password': os.getenv('DB_PASSWORD', '')  # Empty = use .pgpass
        }
        self.conn = None
    
    def connect(self):
        """
        Connect to database with .pgpass support
        
        Password handling:
        1. If password in config - use it
        2. If empty - psycopg uses ~/.pgpass automatically
        """
        user = self.db_config['user']
        password = self.db_config.get('password', '')
        
        # Build connection string
        conn_parts = [
            f"host={self.db_config['host']}",
            f"port={self.db_config['port']}",
            f"dbname={self.db_config['dbname']}",
            f"user={user}"
        ]
        
        # Only add password if provided
        # Otherwise psycopg uses ~/.pgpass
        if password:
            conn_parts.append(f"password={password}")
        
        conn_string = " ".join(conn_parts)
        
        try:
            self.conn = psycopg.connect(
                conn_string,
                row_factory=dict_row,
                connect_timeout=30,
                options='-c statement_timeout=300000'
            )
            logger.info(f"✅ Connected as {user} (pgpass supported)")
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise
        
        return self.conn
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute SELECT query"""
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute INSERT/UPDATE/DELETE"""
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            self.conn.commit()
            return cur.rowcount
    
    def bulk_insert(self, table, columns, data, batch_size=1000):
        """
        Bulk insert with batching and conflict handling
        
        Args:
            table: Table name (schema.table)
            columns: List of column names
            data: List of tuples with values
            batch_size: Rows per batch
            
        Returns:
            Total rows inserted (excluding conflicts)
        """
        if not data:
            return 0
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Add ON CONFLICT DO NOTHING to handle duplicates gracefully
        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (signal_id) DO NOTHING
        """
        
        total = 0
        with self.conn.cursor() as cur:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cur.executemany(query, batch)
                total += cur.rowcount
            self.conn.commit()
        
        return total
    
    def bulk_insert_candles(self, table, columns, data, batch_size=1000):
        """
        Bulk insert candles with correct UNIQUE constraint
        
        Candles table has UNIQUE(pair_symbol, open_time), not signal_id
        """
        if not data:
            return 0
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Correct ON CONFLICT for candles table
        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (pair_symbol, open_time) DO NOTHING
        """
        
        total = 0
        with self.conn.cursor() as cur:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cur.executemany(query, batch)
                total += cur.rowcount
            self.conn.commit()
        
        return total
    
    def bulk_insert_simulations(self, table, columns, data, batch_size=1000):
        """
        Bulk insert simulation results with correct UNIQUE constraint
        
        Simulation table has UNIQUE(signal_id, sl_pct, ts_activation_pct, ts_callback_pct)
        """
        if not data:
            return 0
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Correct ON CONFLICT for simulation_results table
        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (signal_id, sl_pct, ts_activation_pct, ts_callback_pct) DO NOTHING
        """
        
        total = 0
        with self.conn.cursor() as cur:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cur.executemany(query, batch)
                total += cur.rowcount
            self.conn.commit()
        
        return total
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
