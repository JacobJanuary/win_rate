#!/bin/bash
# Project cleanup script - removes test files and keeps only optimization system

echo "🧹 CLEANING PROJECT DIRECTORY"
echo "======================================"
echo ""

# Files to KEEP (optimization system)
KEEP_FILES=(
    "optimization/"
    "migrations/"
    "config/"
    "run_optimization.sh"
    "OPTIMIZATION_README.md"
    "DATABASE_SAFETY.md"
    ".gitignore"
    ".env"
    "requirements.txt"
    "multi_patterns_with_regime.csv"
    "patterns_with_market_regime.csv"
)

echo "Files to keep:"
for file in "${KEEP_FILES[@]}"; do
    echo "  ✅ $file"
done

echo ""
echo "Removing test files..."

# Remove test/research Python scripts
rm -f analyze_*.py
rm -f find_*.py
rm -f correlate_*.py
rm -f calculate_*.py
rm -f explore_*.py
rm -f *_backup.py
rm -f anl_*.py

echo "  ✅ Removed test Python scripts"

# Remove CSV files (except essential ones)
find . -maxdepth 1 -name "*.csv" \
    ! -name "multi_patterns_with_regime.csv" \
    ! -name "patterns_with_market_regime.csv" \
    -delete

echo "  ✅ Removed generated CSV files"

# Remove log files
rm -f *.log
rm -rf logs/*.log 2>/dev/null

echo "  ✅ Removed log files"

# Remove txt files
rm -f *.txt 2>/dev/null

echo "  ✅ Removed txt files"

# Create necessary directories
mkdir -p logs
mkdir -p results
mkdir -p optimization/utils

echo "  ✅ Created necessary directories"

echo ""
echo "======================================"
echo "✅ CLEANUP COMPLETE!"
echo "======================================"
echo ""

# Show what remains
echo "Remaining files:"
ls -1 *.py 2>/dev/null | head -5
ls -1 *.csv 2>/dev/null
echo ""
echo "Directories:"
ls -d */ 2>/dev/null

echo ""
echo "Project is ready for deployment!"
