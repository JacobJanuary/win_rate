#!/bin/bash
# Master script to run entire optimization pipeline

echo "======================================================================================================"
echo "STRATEGY OPTIMIZATION PIPELINE"
echo "======================================================================================================"
echo ""

# Step 1: Extract signals
echo "Step 1: Extracting top multi-pattern signals..."
python3 optimization/extract_top_signals.py
if [ $? -ne 0 ]; then
    echo "❌ Signal extraction failed!"
    exit 1
fi
echo ""

# Step 2: Fetch candles
echo "Step 2: Fetching 1-minute candles from Binance..."
python3 optimization/fetch_binance_candles.py
if [ $? -ne 0 ]; then
    echo "❌ Candle fetching failed!"
    exit 1
fi
echo ""

# Step 3: Run simulations
echo "Step 3: Running trade simulations..."
echo "⚠️  This will take 2-4 hours depending on your CPU"
python3 optimization/simulate_trades.py
if [ $? -ne 0 ]; then
    echo "❌ Simulations failed!"
    exit 1
fi
echo ""

# Step 4: Aggregate results
echo "Step 4: Aggregating results..."
python3 optimization/aggregate_results.py
if [ $? -ne 0 ]; then
    echo "❌ Aggregation failed!"
    exit 1
fi
echo ""

# Step 5: Generate report
echo "Step 5: Generating report..."
python3 optimization/generate_report.py
if [ $? -ne 0 ]; then
    echo "❌ Report generation failed!"
    exit 1
fi
echo ""

echo "======================================================================================================"
echo "✅ PIPELINE COMPLETE!"
echo "======================================================================================================"
echo "Check results/optimization_report.md for findings"
