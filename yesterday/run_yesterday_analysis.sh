#!/bin/bash
# Yesterday Analysis - Full Pipeline Runner
# Regenerates complete yesterday analysis from scratch

set -e  # Exit on any error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "========================================================================================================"
echo "YESTERDAY ANALYSIS - FULL PIPELINE"
echo "========================================================================================================"
echo ""
echo "This will:"
echo "  1. Select signals from 24-48 hours ago (matching top strategies)"
echo "  2. Fetch 1-minute candles from Binance"
echo "  3. Simulate trades with optimized parameters"
echo "  4. Generate performance report"
echo ""
echo "Started at: $(date)"
echo "========================================================================================================"
echo ""

# Parse arguments
MIN_PNL=${1:-180}
POSITION_SIZE=${2:-100}
LEVERAGE=${3:-10}

echo "Parameters:"
echo "  Min Strategy PnL: ${MIN_PNL}%"
echo "  Position Size: \$${POSITION_SIZE}"
echo "  Leverage: ${LEVERAGE}x"
echo ""

# Step 1: Select signals
echo "========================================================================================================"
echo "STEP 1/4: Selecting yesterday's signals..."
echo "========================================================================================================"
python3 yesterday/1_select_yesterday_signals.py --min-pnl "$MIN_PNL"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to select signals"
    exit 1
fi
echo ""

# Step 2: Fetch candles
echo "========================================================================================================"
echo "STEP 2/4: Fetching candles from Binance..."
echo "========================================================================================================"
python3 yesterday/2_fetch_yesterday_candles.py
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to fetch candles"
    exit 1
fi
echo ""

# Step 3: Simulate trades
echo "========================================================================================================"
echo "STEP 3/4: Simulating trades..."
echo "========================================================================================================"
python3 yesterday/3_simulate_yesterday_trades.py
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to simulate trades"
    exit 1
fi
echo ""

# Step 4: Generate report
echo "========================================================================================================"
echo "STEP 4/4: Generating performance report..."
echo "========================================================================================================"
python3 yesterday/4_yesterday_report.py --position-size "$POSITION_SIZE" --leverage "$LEVERAGE"
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to generate report"
    exit 1
fi
echo ""

echo "========================================================================================================"
echo "✅ PIPELINE COMPLETE!"
echo "========================================================================================================"
echo "Finished at: $(date)"
echo ""
echo "Results saved to:"
echo "  - Database: optimization.yesterday_signals, yesterday_candles, yesterday_results"
echo "  - CSV: results/yesterday_performance.csv"
echo ""
echo "To run with custom parameters:"
echo "  ./yesterday/run_yesterday_analysis.sh [MIN_PNL] [POSITION_SIZE] [LEVERAGE]"
echo "  Example: ./yesterday/run_yesterday_analysis.sh 200 150 15"
echo "========================================================================================================"
