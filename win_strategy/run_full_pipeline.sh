#!/bin/bash
# Run Full Win Strategy Pipeline
# Executes all 6 steps in sequence

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Parse arguments
DAYS=30
MIN_WR=60
MIN_SIGNALS=15

while [[ $# -gt 0 ]]; do
    case $1 in
        --days)
            DAYS="$2"
            shift 2
            ;;
        --min-win-rate)
            MIN_WR="$2"
            shift 2
            ;;
        --min-signals)
            MIN_SIGNALS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--days N] [--min-win-rate N] [--min-signals N]"
            exit 1
            ;;
    esac
done

echo "🚀 Running Winning Combinations Pipeline"
echo "========================================"
echo ""
echo "Parameters:"
echo "  Analysis period: $DAYS days"
echo "  Min win rate: $MIN_WR%"
echo "  Min signals: $MIN_SIGNALS"
echo ""
echo "Steps:"
echo "  1. Discover combinations"
echo "  2. Select signals"
echo "  3. Fetch candles"
echo "  4. Simulate trades"
echo "  5. Aggregate results"
echo "  6. Generate report"
echo ""
echo "⏱️  Estimated time: 4-8 hours"
echo ""
read -p "Continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cancelled"
    exit 0
fi

START_TIME=$(date +%s)

# Step 1: Discover combinations
echo ""
echo "================================================"
echo "📊 STEP 1/6: Discover Winning Combinations"
echo "================================================"
echo ""

if python3 1_discover_combinations.py --days "$DAYS" --min-win-rate "$MIN_WR" --min-signals "$MIN_SIGNALS" --rebuild; then
    echo ""
    echo "✅ Step 1 complete"
else
    echo "❌ Error in step 1"
    exit 1
fi

# Step 2: Select signals
echo ""
echo "================================================"
echo "🎯 STEP 2/6: Select Matching Signals"
echo "================================================"
echo ""

if python3 2_select_signals.py --days "$DAYS"; then
    echo ""
    echo "✅ Step 2 complete"
else
    echo "❌ Error in step 2"
    exit 1
fi

# Step 3: Fetch candles
echo ""
echo "================================================"
echo "📈 STEP 3/6: Fetch Candles from Binance"
echo "================================================"
echo ""

if python3 3_fetch_candles.py; then
    echo ""
    echo "✅ Step 3 complete"
else
    echo "❌ Error in step 3"
    exit 1
fi

# Step 4: Simulate trades
echo ""
echo "================================================"
echo "🎲 STEP 4/6: Simulate Trades"
echo "================================================"
echo ""

if python3 4_simulate_trades.py; then
    echo ""
    echo "✅ Step 4 complete"
else
    echo "❌ Error in step 4"
    exit 1
fi

# Step 5: Aggregate results
echo ""
echo "================================================"
echo "📊 STEP 5/6: Aggregate Results"
echo "================================================"
echo ""

if python3 5_aggregate_results.py --days "$DAYS" --min-win-rate "$MIN_WR"; then
    echo ""
    echo "✅ Step 5 complete"
else
    echo "❌ Error in step 5"
    exit 1
fi

# Step 6: Generate report
echo ""
echo "================================================"
echo "📄 STEP 6/6: Generate Report"
echo "================================================"
echo ""

if python3 6_generate_report.py; then
    echo ""
    echo "✅ Step 6 complete"
else
    echo "❌ Error in step 6"
    exit 1
fi

# Final summary
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))

echo ""
echo "================================================"
echo "🎉 ALL STEPS COMPLETE!"
echo "================================================"
echo ""
echo "⏱️  Total time: ${HOURS}h ${MINUTES}min"
echo ""
echo "📊 Check results:"
echo "   - Database: optimization.combination_best_parameters"
echo "   - Report: results/win_strategy_report_*.md"
echo ""
echo "✅ Pipeline complete!"
