# FINM32500-A3: Runtime & Space Complexity in Financial Signal Processing

## Overview
This project implements and analyzes moving average trading strategies with different time and space complexity characteristics:

- **NaiveMovingAverageStrategy**: O(n²) time, O(n) space - recalculates entire window each tick
- **WindowedMovingAverageStrategy**: O(n) time, O(k) space - maintains fixed-size sliding window  
- **OptimizedMovingAverageStrategy**: Optimized version meeting performance requirements

## Summary of Findings

**See `profiler and report.ipynb` for detailed analysis** - contains performance comparisons, complexity analysis, profiling results, and optimization findings from testing all strategy implementations.

## Setup Instructions

### Step 1: Generate Market Data
```bash
python data/data_generator.py
```
This creates three CSV files with real market data:
- `data/market_data_1000.csv` (1,000 ticks)
- `data/market_data_10000.csv` (10,000 ticks)  
- `data/market_data_100000.csv` (100,000 ticks)

### Step 2: Install Dependencies
```bash
pip install psutil  # For system memory monitoring in tests
```

## Running Tests

### Assignment Requirements Testing
The assignment requires 3 specific test categories:

1. **Validate correctness of both strategies**
2. **Confirm optimized strategy runs under 1 second and uses <100MB memory for 100k ticks**
3. **Test that profiling output includes expected hotspots and memory peaks**

### Test Execution Commands

**Run all tests:**
```bash
python -m pytest tests/ -v
```

**Run individual test files:**
```bash
# Strategy correctness validation
python -m pytest tests/test_strategy_correctness.py -v

# Performance requirements validation  
python -m pytest tests/test_performance_validation.py -v

# Profiling validation
python -m pytest tests/test_profiling_validation.py -v
```
