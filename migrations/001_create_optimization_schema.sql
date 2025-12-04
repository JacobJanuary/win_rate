-- Migration: Create Optimization Schema
-- Purpose: Tables for strategy parameter optimization
-- Author: System
-- Date: 2025-12-03

-- Create schema
CREATE SCHEMA IF NOT EXISTS optimization;

-- Table 1: Selected signals for optimization
CREATE TABLE IF NOT EXISTS optimization.selected_signals (
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_signal_id UNIQUE(signal_id)
);

CREATE INDEX IF NOT EXISTS idx_selected_signals_pair_entry ON optimization.selected_signals(pair_symbol, entry_time);
CREATE INDEX IF NOT EXISTS idx_selected_signals_timestamp ON optimization.selected_signals(signal_timestamp);
CREATE INDEX IF NOT EXISTS idx_selected_signals_strategy ON optimization.selected_signals(strategy_name);

-- Table 2: 1-minute candles from Binance
CREATE TABLE IF NOT EXISTS optimization.candles_1m (
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
    
    CONSTRAINT uq_candle_pair_time UNIQUE(pair_symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_candles_pair_time ON optimization.candles_1m(pair_symbol, open_time);
CREATE INDEX IF NOT EXISTS idx_candles_open_time ON optimization.candles_1m(open_time);

-- Table 3: Simulation results for each parameter combination
CREATE TABLE IF NOT EXISTS optimization.simulation_results (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER NOT NULL,
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
    
    CONSTRAINT fk_signal FOREIGN KEY (signal_id) REFERENCES optimization.selected_signals(signal_id) ON DELETE CASCADE,
    CONSTRAINT uq_simulation UNIQUE(signal_id, sl_pct, ts_activation_pct, ts_callback_pct)
);

CREATE INDEX IF NOT EXISTS idx_sim_signal_params ON optimization.simulation_results(signal_id, sl_pct, ts_activation_pct, ts_callback_pct);
CREATE INDEX IF NOT EXISTS idx_sim_params_combo ON optimization.simulation_results(sl_pct, ts_activation_pct, ts_callback_pct);
CREATE INDEX IF NOT EXISTS idx_sim_exit_type ON optimization.simulation_results(exit_type);

-- Table 4: Best parameters aggregated by strategy
CREATE TABLE IF NOT EXISTS optimization.best_parameters (
    id SERIAL PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    signal_type VARCHAR(10) NOT NULL,
    market_regime VARCHAR(20),
    
    sl_pct DECIMAL(5, 2) NOT NULL,
    ts_activation_pct DECIMAL(5, 2) NOT NULL,
    ts_callback_pct DECIMAL(5, 2) NOT NULL,
    
    -- Performance metrics
    total_signals INTEGER,
    winning_trades INTEGER,
    losing_trades INTEGER,
    win_rate DECIMAL(5, 2),
    avg_pnl_pct DECIMAL(10, 4),
    total_pnl_pct DECIMAL(10, 4),
    max_drawdown_pct DECIMAL(10, 4),
    profit_factor DECIMAL(10, 4),
    sharpe_ratio DECIMAL(10, 4),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT uq_best_params UNIQUE(strategy_name, signal_type, market_regime, sl_pct, ts_activation_pct, ts_callback_pct)
);

CREATE INDEX IF NOT EXISTS idx_best_params_strategy ON optimization.best_parameters(strategy_name);
CREATE INDEX IF NOT EXISTS idx_best_params_total_pnl ON optimization.best_parameters(total_pnl_pct DESC);
CREATE INDEX IF NOT EXISTS idx_best_params_win_rate ON optimization.best_parameters(win_rate DESC);

-- Grants (adjust user as needed)
GRANT USAGE ON SCHEMA optimization TO elcrypto_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA optimization TO elcrypto_readonly;

-- Comments
COMMENT ON SCHEMA optimization IS 'Strategy parameter optimization data';
COMMENT ON TABLE optimization.selected_signals IS 'Top multi-pattern signals selected for optimization';
COMMENT ON TABLE optimization.candles_1m IS '1-minute candles from Binance for simulation';
COMMENT ON TABLE optimization.simulation_results IS 'Results of trade simulations with various SL/TS parameters';
COMMENT ON TABLE optimization.best_parameters IS 'Best performing parameter combinations per strategy';
