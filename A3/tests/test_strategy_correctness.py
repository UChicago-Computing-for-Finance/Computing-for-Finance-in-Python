"""
Unit Tests for Strategy Correctness
ASSIGNMENT REQUIREMENT: Validate correctness of both strategies
"""

import unittest
import sys
import os
import csv
from datetime import datetime
from typing import List

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import MarketDataPoint, Signal
from strategies.NaiveMovingAverageStrategy import NaiveMovingAverageStrategy, OptimizedMovingAverageStrategy
from strategies.WindowedMovingAverageStrategy import WindowedMovingAverageStrategy


class TestStrategyCorrectness(unittest.TestCase):
    """Test that all strategy implementations produce identical results"""
    
    def setUp(self):
        """Set up test data and strategy instances"""
        self.symbol = "AAPL"
        self.capital = 10000.0
        self.window_size = 40
        
        # Use simple data loader
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from data_loader import load_test_data
        self.test_data = load_test_data('1000')
        print(f"Loaded {len(self.test_data)} real market data points for testing")
        
        # Initialize strategies with same parameters
        self.naive_strategy = NaiveMovingAverageStrategy(self.symbol, self.capital, self.window_size)
        self.windowed_strategy = WindowedMovingAverageStrategy(self.symbol, self.capital, self.window_size)
        self.optimized_strategy = OptimizedMovingAverageStrategy(self.symbol, self.capital, self.window_size)
    
    def _run_strategy_on_data(self, strategy, data: List[MarketDataPoint]) -> List[Signal]:
        """Helper to run a strategy on test data and collect all signals"""
        all_signals = []
        for tick in data:
            signals = strategy.generate_signals(tick, max_order_vol=1000.0)
            all_signals.extend(signals)
        return all_signals
    
    def test_naive_vs_windowed_signals(self):
        """Test that naive and windowed strategies produce identical signals"""
        naive_signals = self._run_strategy_on_data(self.naive_strategy, self.test_data)
        windowed_signals = self._run_strategy_on_data(self.windowed_strategy, self.test_data)
        
        # Should have same number of signals
        self.assertEqual(len(naive_signals), len(windowed_signals), 
                        "Naive and windowed strategies should produce same number of signals")
        
        # Compare each signal
        for i, (naive_sig, windowed_sig) in enumerate(zip(naive_signals, windowed_signals)):
            with self.subTest(signal_index=i):
                self.assertEqual(naive_sig.timestamp, windowed_sig.timestamp, "Signal timestamps should match")
                self.assertEqual(naive_sig.symbol, windowed_sig.symbol, "Signal symbols should match")
                self.assertEqual(naive_sig.side, windowed_sig.side, "Signal sides should match")
                self.assertEqual(naive_sig.quantity, windowed_sig.quantity, "Signal quantities should match")
    
    def test_optimized_vs_naive_signals(self):
        """Test that optimized strategy produces identical signals to naive"""
        naive_signals = self._run_strategy_on_data(self.naive_strategy, self.test_data)
        optimized_signals = self._run_strategy_on_data(self.optimized_strategy, self.test_data)
        
        self.assertEqual(len(naive_signals), len(optimized_signals), 
                        "Optimized and naive strategies should produce same number of signals")
        
        for i, (naive_sig, opt_sig) in enumerate(zip(naive_signals, optimized_signals)):
            with self.subTest(signal_index=i):
                self.assertEqual(naive_sig.timestamp, opt_sig.timestamp, "Signal timestamps should match")
                self.assertEqual(naive_sig.side, opt_sig.side, "Signal sides should match")
                self.assertEqual(naive_sig.quantity, opt_sig.quantity, "Signal quantities should match")


if __name__ == '__main__':
    unittest.main(verbosity=2)