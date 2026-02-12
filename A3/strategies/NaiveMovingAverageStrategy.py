from models import MarketDataPoint, Signal, Strategy
from typing import List, Optional
import logging

class NaiveMovingAverageStrategy(Strategy):
    """
    Short if price above MA buy one share each time, long if price below MA sell one share each time.
    
    TIME COMPLEXITY: O(n²) overall - Each tick requires O(n) computation, with n ticks total
    SPACE COMPLEXITY: O(n) - Stores all historical prices in memory (unbounded growth)
    """
    
    def __init__(self, symbol: str, capital: float, window_size: int = 40):
        self._symbol = symbol
        self._remaining_capital = capital
        self._window_size = window_size
        self._current_ma = 0
        self._previous_ma = 0
        # SPACE COMPLEXITY: O(n) - List grows unbounded with each new price point
        # This is the key inefficiency: storing ALL historical data forever
        self._price_history = []
        super().__init__(symbol, capital)


        # logging:
        # Set up logger for this strategy
        self.logger = logging.getLogger(f"Strategy_{self._symbol}")
        self.logger.setLevel(logging.INFO)
        
        # Create handler if it doesn't exist
        if not self.logger.handlers:
            handler = logging.FileHandler(f"logs/strategy_{self._symbol}.log")
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.logger.propagate = False


    def _calculate_ma(self) -> Optional[float]:
        # TIME COMPLEXITY: O(n) where n = len(self._price_history)
        # This is the core inefficiency: recalculates from scratch every time
        if len(self._price_history) == 0:
            return 0

        window_size = self._window_size

        # O(min(n, window_size)) slice operation - still O(n) in worst case
        data = self._price_history[-window_size:] if len(self._price_history) > window_size else self._price_history
        
        # O(min(n, window_size)) sum operation - iterates through all values
        return sum(data) / len(data)
        

    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float) -> List[Signal]:

        if tick.symbol != self._symbol:
            return []

        out: List[Signal] = []
        
        # SPACE COMPLEXITY: O(1) operation - just appending one element
        # But overall list grows by O(1) per call, leading to O(n) total space
        self._price_history.append(tick.price)
        
        # TIME COMPLEXITY: O(n) per call - recalculates MA from scratch
        # With n total calls, this gives O(n²) overall time complexity
        current_ma = self._calculate_ma()

        # logging
        self.logger.info(f"Tick {len(self._price_history)}: Price=${tick.price:.2f}, MA=${current_ma:.2f}, History Length={len(self._price_history)}")
        
        # Need both SMAs to be available and previous values for crossover detection
        if (current_ma == 0 or self._previous_ma == 0):
            # Store current values for next iteration
            self._previous_ma = current_ma
            return out
        
        qty = 1 # for this assignment, just trade 1 share at a time

        #Check for crossover signals
        #BUY: current price above MA 
        #SELL: current price below MA


        if (tick.price > self._previous_ma ):
            signal = Signal( tick.timestamp, tick.symbol, "BUY", qty, reason=f"MA crossover: {current_ma:.2f} > {tick.price:.2f}", strategy="MA_CROSSOVER")
            out.append(signal)

            self.logger.info(f"  -> BUY SIGNAL: Price ${tick.price:.2f} > Prev MA ${self._previous_ma:.2f} | Current MA: ${current_ma:.2f}")
        
        # SELL: short SMA crosses below long SMA
        elif (tick.price < self._previous_ma):

            signal = Signal( tick.timestamp, tick.symbol, "SELL", qty, reason=f"MA crossover: {current_ma:.2f} < {tick.price:.2f}", strategy="MA_CROSSOVER" )
            out.append(signal)
            self.logger.info(f"  -> SELL SIGNAL: Price ${tick.price:.2f} < Prev MA ${self._previous_ma:.2f} | Current MA: ${current_ma:.2f}")

        else:
            self.logger.info(f"  -> NO SIGNAL: Price ${tick.price:.2f} = Prev MA ${self._previous_ma:.2f} | Current MA: ${current_ma:.2f}")
        
        # Store current values for next iteration
        self._previous_ma = current_ma
        
        return out
    
    @property
    def remaining_capital(self):
        return self._remaining_capital

    @remaining_capital.setter
    def remaining_capital(self, value: float):
        self._remaining_capital = value


class OptimizedMovingAverageStrategy(Strategy):
    """
    HIGHLY OPTIMIZED VERSION focusing on speed for 100k+ ticks:
    
    1. Minimal object creation and method calls
    2. No logging during signal generation
    3. Direct arithmetic operations 
    4. Circular buffer for O(1) operations
    
    TIME COMPLEXITY: O(n) overall - O(1) per tick
    SPACE COMPLEXITY: O(k) - Only stores window_size prices
    """
    
    def __init__(self, symbol: str, capital: float, window_size: int = 40):
        self._symbol = symbol
        self._remaining_capital = capital
        self._window_size = window_size
        self._previous_ma = 0.0
        
        # Circular buffer for O(1) operations
        self._prices = [0.0] * window_size
        self._index = 0
        self._count = 0
        self._running_sum = 0.0
        
        super().__init__(symbol, capital)

    def _update_ma(self, new_price: float) -> float:
        """Ultra-fast moving average with circular buffer O(1) operations"""
        if self._count < self._window_size:
            # Still filling the buffer
            self._prices[self._index] = new_price
            self._running_sum += new_price
            self._count += 1
            self._index = (self._index + 1) % self._window_size
            return self._running_sum / self._count
        else:
            # Buffer is full - replace oldest value
            old_price = self._prices[self._index]
            self._prices[self._index] = new_price
            self._running_sum += new_price - old_price
            self._index = (self._index + 1) % self._window_size
            return self._running_sum / self._window_size

    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float) -> List[Signal]:
        """Ultra-fast signal generation with minimal overhead"""
        if tick.symbol != self._symbol:
            return []

        # Fast moving average update
        current_ma = self._update_ma(tick.price)
        
        # Quick signal generation with minimal object creation
        if self._count < 2:
            self._previous_ma = current_ma
            return []

        # Direct comparison without logging overhead
        price = tick.price
        prev_ma = self._previous_ma
        
        if price > prev_ma:
            # BUY signal
            self._previous_ma = current_ma
            return [Signal(tick.timestamp, self._symbol, "BUY", 1)]
        elif price < prev_ma:
            # SELL signal  
            self._previous_ma = current_ma
            return [Signal(tick.timestamp, self._symbol, "SELL", 1)]
        else:
            # No signal
            self._previous_ma = current_ma
            return []

    def get_memory_info(self) -> dict:
        """Memory usage information for testing"""
        return {
            'window_size': self._count,
            'max_window_size': self._window_size,
            'is_full': self._count == self._window_size
        }

    @property
    def remaining_capital(self):
        return self._remaining_capital

    @remaining_capital.setter
    def remaining_capital(self, value: float):
        self._remaining_capital = value

    def get_memory_info(self) -> dict:
        """Memory usage information for testing"""
        return {
            'window_size': self._count,
            'max_window_size': self._window_size,
            'is_full': self._count == self._window_size
        }

    @property
    def remaining_capital(self):
        return self._remaining_capital

    @remaining_capital.setter
    def remaining_capital(self, value: float):
        self._remaining_capital = value
    
    @property
    def remaining_capital(self):
        return self._remaining_capital

    @remaining_capital.setter
    def remaining_capital(self, value: float):
        self._remaining_capital = value

