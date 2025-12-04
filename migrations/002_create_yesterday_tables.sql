-- Migration: Create Yesterday Analysis Tables
-- Purpose: Tables for testing optimized parameters on recent signals (24-48hrs old)
-- Author: System
-- Date: 2025-12-04

-- Table 1: Yesterday's selected signals
CREATE TABLE IF NOT EXISTS optimization.yesterday_signals (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER NOT NULL,
    pair_symbol VARCHAR(20) NOT NULL,
    signal_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    entry_time TIMESTAMP WITH TIME ZONE NOT NULL,  -- signal_timestamp + 17 minutes
    signal_type VARCHAR(10) NOT NULL,  -- LONG/SHORT
    patterns TEXT[] NOT NULL,
    market_regime VARCHAR(20),
    total_score DECIMAL,
    strategy_name TEXT,
    
    -- Optimized parameters (from best_parameters)
    sl_pct DECIMAL(5, 2) NOT NULL,
    ts_activation_pct DECIMAL(5, 2) NOT NULL,
    ts_callback_pct DECIMAL(5, 2) NOT NULL,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_yesterday_signal_id UNIQUE(signal_id)
);

CREATE INDEX IF NOT EXISTS idx_yesterday_signals_timestamp ON optimization.yesterday_signals(signal_timestamp);
CREATE INDEX IF NOT EXISTS idx_yesterday_signals_strategy ON optimization.yesterday_signals(strategy_name);

-- Table 2: Yesterday's 1-minute candles
CREATE TABLE IF NOT EXISTS optimization.yesterday_candles (
    id SERIAL PRIMARY KEY,
    pair_symbol VARCHAR(20) NOT NULL,
    open_time BIGINT NOT NULL,  -- Unix timestamp milliseconds
    open_price DECIMAL(20, 8) NOT NULL,
    high_price DECIMAL(20, 8) NOT NULL,
    low_price DECIMAL(20, 8) NOT NULL,
    close_price DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    close_time BIGINT NOT NULL,
    quote_volume DECIMAL(20, 8),
    trades_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_yesterday_candle UNIQUE(pair_symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_yesterday_candles_pair_time ON optimization.yesterday_candles(pair_symbol, open_time);

-- Table 3: Yesterday's simulation results
CREATE TABLE IF NOT EXISTS optimization.yesterday_results (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER NOT NULL,
    
    -- Simulation parameters (from yesterday_signals)
    sl_pct DECIMAL(5, 2) NOT NULL,
    ts_activation_pct DECIMAL(5, 2) NOT NULL,
    ts_callback_pct DECIMAL(5, 2) NOT NULL,
    
    -- Results
    exit_type VARCHAR(20),  -- TP, SL, TS, TIME_LIMIT
    exit_price DECIMAL(20, 8),
    exit_time BIGINT,  -- Unix timestamp
    pnl_pct DECIMAL(10, 4),
    max_profit_pct DECIMAL(10, 4),
    max_drawdown_pct DECIMAL(10, 4),
    hold_duration_minutes INTEGER,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_yesterday_signal FOREIGN KEY (signal_id) 
        REFERENCES optimization.yesterday_signals(signal_id) ON DELETE CASCADE,
    CONSTRAINT uq_yesterday_result UNIQUE(signal_id)
);

CREATE INDEX IF NOT EXISTS idx_yesterday_results_signal ON optimization.yesterday_results(signal_id);
CREATE INDEX IF NOT EXISTS idx_yesterday_results_exit_type ON optimization.yesterday_results(exit_type);

-- Grants
GRANT SELECT ON ALL TABLES IN SCHEMA optimization TO elcrypto_readonly;

-- Comments
COMMENT ON TABLE optimization.yesterday_signals IS 'Signals from 24-48hrs ago with optimized parameters';
COMMENT ON TABLE optimization.yesterday_candles IS '1-minute candles for yesterday analysis';
COMMENT ON TABLE optimization.yesterday_results IS 'Simulation results for yesterday signals';
