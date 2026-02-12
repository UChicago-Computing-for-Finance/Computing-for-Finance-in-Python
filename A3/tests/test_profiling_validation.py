"""
Profiling Validation Tests
ASSIGNMENT REQUIREMENT: Test that profiling output includes expected hotspots and memory peaks
"""

import unittest
import sys
import os
import cProfile
import pstats
import io
import tracemalloc
from typing import List

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import MarketDataPoint
from strategies.NaiveMovingAverageStrategy import OptimizedMovingAverageStrategy


class TestProfilingValidation(unittest.TestCase):
    """Test that profiling identifies expected hotspots and memory patterns"""
    
    def setUp(self):
        """Set up test parameters"""
        self.symbol = "AAPL"
        self.capital = 10000.0
        self.window_size = 40
        
        # Use simple data loader
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from data_loader import load_test_data
        self.load_test_data = load_test_data
    
    def test_profiling_identifies_hotspots(self):
        """ASSIGNMENT REQUIREMENT: Verify profiling identifies expected hotspots"""
        print(f"\n=== Testing Profiling Hotspot Detection ===")
        
        # Load test data
        test_data = self.load_test_data('10000')
        print(f"Using {len(test_data)} real market data points for profiling")
        
        # Set up profiling
        profiler = cProfile.Profile()
        strategy = OptimizedMovingAverageStrategy(self.symbol, self.capital, self.window_size)
        
        # Profile the strategy execution
        profiler.enable()
        total_signals = 0
        for tick in test_data:
            signals = strategy.generate_signals(tick, max_order_vol=1000.0)
            total_signals += len(signals)
        profiler.disable()
        
        # Analyze profiling results
        stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stream)
        stats.sort_stats('tottime')
        stats.print_stats()
        
        profile_output = stream.getvalue()
        print("Top functions by total time:")
        print(profile_output[:2000])  # Print first 2000 chars
        
        # ASSIGNMENT REQUIREMENT: Verify expected hotspots are identified
        expected_hotspots = [
            'generate_signals',  # Core strategy method
            'calculate_moving_average',  # Moving average computation
            'append'  # List operations for windowing
        ]
        
        hotspots_found = []
        for hotspot in expected_hotspots:
            if hotspot in profile_output:
                hotspots_found.append(hotspot)
                print(f"✅ Found expected hotspot: {hotspot}")
            else:
                print(f"❌ Missing expected hotspot: {hotspot}")
        
        # Verify that we found the key hotspots
        self.assertGreater(len(hotspots_found), 0, "Should identify at least some expected hotspots")
        self.assertIn('generate_signals', hotspots_found, "Should identify generate_signals as hotspot")
        
        # Verify strategy processed data successfully
        self.assertGreater(total_signals, 0, "Should generate some trading signals")
        print(f"Successfully profiled strategy execution with {total_signals} signals generated")
    
    def test_memory_peak_detection(self):
        """ASSIGNMENT REQUIREMENT: Verify profiling identifies memory peaks"""
        print(f"\n=== Testing Memory Peak Detection ===")
        
        # Load test data
        test_data = self.load_test_data('10000')
        print(f"Using {len(test_data)} real market data points for memory profiling")
        
        # Start memory tracking
        tracemalloc.start()
        
        strategy = OptimizedMovingAverageStrategy(self.symbol, self.capital, self.window_size)
        
        # Track memory usage during execution
        memory_snapshots = []
        total_signals = 0
        
        # Take memory snapshot every 1000 ticks
        for i, tick in enumerate(test_data):
            signals = strategy.generate_signals(tick, max_order_vol=1000.0)
            total_signals += len(signals)
            
            if i % 1000 == 0:
                current, peak = tracemalloc.get_traced_memory()
                memory_snapshots.append({
                    'tick': i,
                    'current_mb': current / (1024 * 1024),
                    'peak_mb': peak / (1024 * 1024)
                })
        
        # Final memory snapshot
        current, peak = tracemalloc.get_traced_memory()
        memory_snapshots.append({
            'tick': len(test_data),
            'current_mb': current / (1024 * 1024),
            'peak_mb': peak / (1024 * 1024)
        })
        
        tracemalloc.stop()
        
        # Analyze memory patterns
        print("Memory usage snapshots:")
        for snapshot in memory_snapshots:
            print(f"Tick {snapshot['tick']:,}: Current {snapshot['current_mb']:.1f}MB, "
                  f"Peak {snapshot['peak_mb']:.1f}MB")
        
        # ASSIGNMENT REQUIREMENT: Verify memory peaks are identified
        final_peak = memory_snapshots[-1]['peak_mb']
        self.assertGreater(final_peak, 0, "Should detect memory usage")
        
        # Check that memory usage is reasonable (under 100MB as per assignment)
        self.assertLess(final_peak, 100.0, f"Memory peak {final_peak:.1f}MB should be under 100MB")
        
        # Verify memory patterns make sense
        memory_values = [s['current_mb'] for s in memory_snapshots]
        self.assertGreater(max(memory_values), 0, "Should show memory usage")
        
        print(f"✅ Memory profiling detected peak usage: {final_peak:.1f}MB")
        print(f"Successfully tracked memory during execution with {total_signals} signals generated")


if __name__ == '__main__':
    unittest.main(verbosity=2)