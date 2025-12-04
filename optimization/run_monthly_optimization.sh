#!/bin/bash
# Monthly Optimization - Full Pipeline Runner
# Updates or rebuilds complete monthly optimization analysis

set -e  # Exit on any error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================================================"
echo "MONTHLY OPTIMIZATION - FULL PIPELINE"
echo "========================================================================================================"
echo ""
echo "This will:"
echo "  1. Extract signals for top 20 strategies (incremental or rebuild)"
echo "  2. Fetch missing 1-minute candles from Binance"
echo "  3. Simulate trades for new signals"
echo "  4. Aggregate results and find best parameters"
echo "  5. Generate detailed performance report"
echo ""
echo "Started at: $(date)"
echo "========================================================================================================"
echo ""

# Parse mode flag
MODE="incremental"
if [[ "$1" == "--rebuild" ]]; then
    MODE="rebuild"
    echo "⚠️  REBUILD MODE: Will delete existing data and start fresh"
    echo ""
fi

echo "Mode: $MODE"
echo ""

# Step 1: Extract signals
echo "========================================================================================================"
echo "STEP 1/5: Extracting signals for top strategies..."
echo "========================================================================================================"
if [[ "$MODE" == "rebuild" ]]; then
    python3 optimization/extract_top_signals.py --rebuild
else
    # Incremental mode - signals table uses ON CONFLICT DO NOTHING
    python3 optimization/extract_top_signals.py
fi
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to extract signals"
    exit 1
fi
echo ""

# Step 2: Fetch candles
echo "========================================================================================================"
echo "STEP 2/5: Fetching candles from Binance..."
echo "========================================================================================================"
echo "Note: Automatically skips already fetched candles (incremental)"
python3 optimization/fetch_binance_candles.py
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to fetch candles"
    exit 1
fi
echo ""

# Step 3: Simulate trades
echo "========================================================================================================"
echo "STEP 3/5: Simulating trades..."
echo "========================================================================================================"
echo "Note: Automatically skips already simulated signals (incremental)"
python3 optimization/simulate_trades.py
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to simulate trades"
    exit 1
fi
echo ""

# Step 4: Aggregate results
echo "========================================================================================================"
echo "STEP 4/5: Aggregating results..."
echo "========================================================================================================"
python3 optimization/aggregate_results.py
if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to aggregate results"
    exit 1
fi
echo ""

# Step 5: Detailed report
echo "========================================================================================================"
echo "STEP 5/5: Generating detailed strategy report..."
echo "========================================================================================================"
python3 optimization/detailed_strategy_report.py
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
echo "  - Database: optimization.selected_signals, candles_1m, simulation_results, best_parameters"
echo "  - CSV: results/aggregated_results.csv, results/detailed_strategy_report.csv"
echo ""
echo "Summary:"
SIGNAL_COUNT=$(psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -t -c "SELECT COUNT(*) FROM optimization.selected_signals" 2>/dev/null | xargs || echo "N/A")
SIM_COUNT=$(psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -t -c "SELECT COUNT(*) FROM optimization.simulation_results" 2>/dev/null | xargs || echo "N/A")
STRAT_COUNT=$(psql -h localhost -p 5433 -U elcrypto -d fox_crypto_new -t -c "SELECT COUNT(DISTINCT strategy_name) FROM optimization.best_parameters" 2>/dev/null | xargs || echo "N/A")
echo "  Total Signals: $SIGNAL_COUNT"
echo "  Simulations: $SIM_COUNT"
echo "  Strategies Analyzed: $STRAT_COUNT"
echo ""
echo "Usage:"
echo "  Incremental update: ./optimization/run_monthly_optimization.sh"
echo "  Full rebuild:       ./optimization/run_monthly_optimization.sh --rebuild"
echo "========================================================================================================"
