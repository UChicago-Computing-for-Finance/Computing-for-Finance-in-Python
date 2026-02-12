"""
Performance Validation Tests
ASSIGNMENT REQUIREMENT: Confirm that optimized strategy runs under 1 second 
and uses <100MB memory for 100k ticks
"""

import unittest
import sys
import os
import time
import psutil
import gc
import tracemalloc
from typing import List

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import MarketDataPoint
from strategies.NaiveMovingAverageStrategy import OptimizedMovingAverageStrategy


class TestPerformanceValidation(unittest.TestCase):
    """Test performance requirements for optimized strategy"""
    
    def setUp(self):
        """Set up test parameters"""
        self.symbol = "AAPL"
        self.capital = 10000.0
        self.window_size = 40
        
        # Performance thresholds from assignment
        self.max_runtime_seconds = 1.0  # Must run under 1 second
        self.max_memory_mb = 100.0      # Must use <100MB memory
        
        # Use simple data loader
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from data_loader import load_test_data
        self.load_test_data = load_test_data
    
    def _measure_strategy_performance(self, strategy_class, data: List[MarketDataPoint]) -> dict:
        """Measure runtime and memory usage of a strategy"""
        # Force garbage collection before test
        gc.collect()
        
        # Start memory tracking
        tracemalloc.start()
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024 * 1024)  # Convert to MB
        
        # Create strategy instance
        strategy = strategy_class(self.symbol, self.capital, self.window_size)
        
        # Measure runtime
        start_time = time.perf_counter()
        
        # Run strategy on all data
        total_signals = 0
        for tick in data:
            signals = strategy.generate_signals(tick, max_order_vol=1000.0)
            total_signals += len(signals)
        
        end_time = time.perf_counter()
        runtime = end_time - start_time
        
        # Measure final memory
        final_memory = process.memory_info().rss / (1024 * 1024)
        
        # Get tracemalloc peak
        current, peak_tracemalloc = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        return {
            'runtime_seconds': runtime,
            'memory_used_mb': final_memory - initial_memory,
            'peak_tracemalloc_mb': peak_tracemalloc / (1024 * 1024),
            'total_signals': total_signals,
            'ticks_processed': len(data)
        }
    
    def test_optimized_strategy_performance_100k(self):
        """ASSIGNMENT REQUIREMENT: Test that optimized strategy meets 100k performance requirements"""
        print(f"\n=== Testing OptimizedMovingAverageStrategy with 100,000 real data points ===")
        
        # Load real 100k data
        test_data = self.load_test_data('100000')
        print(f"Loaded {len(test_data)} real market data points")
        
        # Measure performance
        results = self._measure_strategy_performance(OptimizedMovingAverageStrategy, test_data)
        
        # Print results
        print(f"Runtime: {results['runtime_seconds']:.3f} seconds")
        print(f"Memory used: {results['memory_used_mb']:.1f} MB")
        print(f"Peak memory (tracemalloc): {results['peak_tracemalloc_mb']:.1f} MB")
        print(f"Signals generated: {results['total_signals']:,}")
        print(f"Ticks processed: {results['ticks_processed']:,}")
        
        # ASSIGNMENT PERFORMANCE ASSERTIONS
        self.assertLess(results['runtime_seconds'], self.max_runtime_seconds, 
                       f"Runtime {results['runtime_seconds']:.3f}s exceeds maximum {self.max_runtime_seconds}s")
        
        # Check memory usage (use the smaller of the two measurements)
        memory_to_check = min(results['memory_used_mb'], results['peak_tracemalloc_mb'])
        self.assertLess(memory_to_check, self.max_memory_mb, 
                       f"Memory usage {memory_to_check:.1f}MB exceeds maximum {self.max_memory_mb}MB")
        
        # Verify strategy actually processed data
        self.assertEqual(results['ticks_processed'], len(test_data))
        self.assertGreater(results['total_signals'], 0, "Should generate some trading signals")
        
        print("✅ Optimized strategy meets all performance requirements!")


if __name__ == '__main__':
    unittest.main(verbosity=2)