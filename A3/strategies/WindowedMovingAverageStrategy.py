from models import MarketDataPoint, Signal, Strategy
from typing import List, Optional
from collections import deque
import logging

class WindowedMovingAverageStrategy(Strategy):
    """
    Short if price above MA buy one share each time, long if price below MA sell one share each time.
    
    TIME COMPLEXITY: O(n) overall - Each tick requires O(1) computation, with n ticks total
    SPACE COMPLEXITY: O(k) - Only stores k prices in sliding window (constant memory)
    where k = window_size (typically much smaller than total data points n)
    """
    
    def __init__(self, symbol: str, capital: float, window_size: int = 40):
        self._symbol = symbol
        self._remaining_capital = capital
        self._window_size = window_size
        self._current_ma = 0
        # SPACE COMPLEXITY: O(1) - Running sum, no additional storage needed
        self._price_sum = 0
        self._price_count = 0
        self._previous_ma = 0
        
        # SPACE COMPLEXITY: O(k) where k = window_size
        # Key optimization: bounded memory usage regardless of total data size
        # deque with maxlen automatically removes oldest when full
        self._price_history = deque(maxlen=window_size)

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


    def _calculate_ma(self, price: float) -> Optional[float]:
        # TIME COMPLEXITY: O(1) - Constant time moving average update
        # This is the key optimization: incremental calculation instead of recomputation

        # O(1) - Check if deque is at capacity
        old_price = None
        if len(self._price_history) == self._window_size:
            old_price = self._price_history[0]  # O(1) - Access first element in deque
        
        # O(1) - Append to deque (automatically removes oldest if maxlen reached)
        self._price_history.append(price)
        
        # O(1) - Update running sum incrementally (no iteration needed!)
        if old_price is not None:
            # Window is full: subtract old price, add new price - O(1)
            self._price_sum = self._price_sum - old_price + price
        else:
            # Window not full yet: just add new price - O(1)
            self._price_sum += price
        
        # O(1) - Simple arithmetic to get average
        return self._price_sum / len(self._price_history)
        

    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float) -> List[Signal]:

        if tick.symbol != self._symbol:
            return []

        out: List[Signal] = []
        
        # TIME COMPLEXITY: O(1) per call - incremental moving average calculation
        # SPACE COMPLEXITY: O(1) per call - bounded memory usage
        # Calculate current MA (this method handles adding the price to history)
        current_ma = self._calculate_ma(tick.price)

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

