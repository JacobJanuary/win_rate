# Yesterday Analysis System

## Overview
This system tests optimized strategy parameters on recent signals (24-48 hours old) to validate performance.

## Prerequisites
1. Run main optimization pipeline first (`optimization/` folder)
2. Ensure `optimization.best_parameters` table is populated
3. Apply migration: `migrations/002_create_yesterday_tables.sql`

## Pipeline Steps

### 1. Apply Database Migration
```bash
psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -f migrations/002_create_yesterday_tables.sql
```

### 2. Select Yesterday's Signals
```bash
python3 yesterday/1_select_yesterday_signals.py --min-pnl 180
```
**What it does:**
- Finds signals from 24-48 hours ago
- Filters by top strategies (total_pnl > 180%)
- Includes optimized parameters (SL, TS) from `best_parameters`
- Saves to `optimization.yesterday_signals`

**Output:** Number of signals selected by strategy

### 3. Fetch Candles from Binance
```bash
python3 yesterday/2_fetch_yesterday_candles.py
```
**What it does:**
- Downloads 1-minute candles for each signal
- 24 hours of data per signal (1440 candles)
- Saves to `optimization.yesterday_candles`
- Rate limit: 200 req/min

**Time:** ~0.3s per signal

### 4. Simulate Trades
```bash
python3 yesterday/3_simulate_yesterday_trades.py
```
**What it does:**
- Runs trade simulation with optimized SL/TS parameters
- Calculates exact exit points and P&L
- Saves results to `optimization.yesterday_results`

**Output:** Success/failure count

### 5. Generate Report
```bash
python3 yesterday/4_yesterday_report.py --position-size 100 --leverage 10
```
**What it does:**
- Analyzes all simulation results
- Calculates capital requirements with leverage
- Shows performance metrics
- Saves CSV report

**Output:**
- Win rate, Total PnL, Exit breakdown
- Capital required (with leverage)
- ROI calculation
- Top performing strategies

## Quick Start - Run All Steps
```bash
# Apply migration (once)
psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -f migrations/002_create_yesterday_tables.sql

# Run pipeline
python3 yesterday/1_select_yesterday_signals.py
python3 yesterday/2_fetch_yesterday_candles.py
python3 yesterday/3_simulate_yesterday_trades.py
python3 yesterday/4_yesterday_report.py
```

## Parameters

### 1_select_yesterday_signals.py
- `--min-pnl`: Minimum strategy total_pnl (default: 180%)

### 4_yesterday_report.py
- `--position-size`: Position size in USD (default: 100)
- `--leverage`: Leverage multiplier (default: 10x)

## Database Tables

### optimization.yesterday_signals
- Selected signals with optimized parameters
- UNIQUE constraint: `signal_id`

### optimization.yesterday_candles  
- 1-minute candles for simulation
- UNIQUE constraint: `(pair_symbol, open_time)`

### optimization.yesterday_results
- Simulation results (one per signal)
- UNIQUE constraint: `signal_id`

## Output Files
- `results/yesterday_performance.csv` - Full results with all details

## Notes
- Signals must be 24-48 hours old (completed trading window)
- Only Binance Futures pairs (exchange_id=1, contract_type_id=1)
- Uses same simulation logic as main optimization pipeline
- Leverage reduces capital requirement by 10x (default)

## Troubleshooting

**No signals found:**
- Check if main optimization was run
- Verify `best_parameters` has strategies with total_pnl > 180%
- Ensure there are signals 24-48hrs old in `fas_v2.scoring_history`

**Candle fetch errors:**
- Some pairs may be delisted (handled gracefully)
- Check Binance API rate limits
- Verify pairs are on Binance Futures

**Simulation failures:**
- Ensure candles were downloaded successfully  
- Check that entry_time is within candle time range

## Example Output
```
YESTERDAY PERFORMANCE REPORT
========================================================

📅 TIME WINDOW: 2025-11-30 to 2025-12-01

💵 TRADING PARAMETERS:
   Position Size: $100
   Leverage: 10x
   Margin per trade: $10.00

📊 PERFORMANCE:
   Total Trades: 150
   Winning: 78 (52.0%)
   Losing: 72 (48.0%)

💰 PnL METRICS:
   Total PnL: 165.50%
   Total Profit (USD): $165.50

💼 CAPITAL REQUIREMENTS:
   Required Capital (with 10x leverage): $520.00
   ROI (on required capital): 31.83%
```
