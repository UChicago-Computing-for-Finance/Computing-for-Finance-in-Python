from models import MarketDataPoint, Signal, Strategy
from typing import List, Optional
from collections import deque

class MovingAverageStrategy(Strategy):
    """
    Short SMA vs Long SMA crossover.
    BUY when short SMA crosses above long SMA.
    SELL when short SMA crosses below long SMA.
    """
    
    def __init__(self, symbol: str, capital: float, short_window: int = 20, long_window: int = 50 ):
        self._symbol = symbol
        self._remaining_capital = capital
        self._short_window = short_window
        self._long_window = long_window
        self._price_history = deque(maxlen=long_window)
        self._previous_short_sma = None
        self._previous_long_sma = None
        super().__init__(symbol, capital)


    def _calculate_sma(self, window: int) -> Optional[float]:
        """Calculate Simple Moving Average for the given window."""
        if len(self._price_history) < window:
            return None
        # Sum the last `window` elements by iterating from the right.
        s = 0.0
        count = 0
        for price in reversed(self._price_history):
            s += price
            count += 1
            if count == window:
                break
        return s / window

    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float = None) -> List[Signal]:
        if tick.symbol != self._symbol:
            return []

        out: List[Signal] = []
        
        # Add current price to history
        self._price_history.append(tick.price)
        
        # Calculate current SMAs
        current_short_sma = self._calculate_sma(self._short_window)
        current_long_sma = self._calculate_sma(self._long_window)
        
        # Need both SMAs to be available and previous values for crossover detection
        if (current_short_sma is None or current_long_sma is None or 
            self._previous_short_sma is None or self._previous_long_sma is None):
            # Store current values for next iteration
            self._previous_short_sma = current_short_sma
            self._previous_long_sma = current_long_sma
            return out
        
        #volume = min(self._remaining_capital, max_order_vol)
        #qty = int(volume / tick.price)
        qty = 1 # for this assignment, just trade 1 share at a time

        #Check for crossover signals
        #BUY: short SMA crosses above long SMA

        if (self._previous_short_sma <= self._previous_long_sma and 
            current_short_sma > current_long_sma):
            out.append(Signal( tick.timestamp, tick.symbol, "BUY", qty, reason=f"SMA crossover: {current_short_sma:.2f} > {current_long_sma:.2f}", strategy="SMA_CROSSOVER"))
        
        # SELL: short SMA crosses below long SMA
        elif (self._previous_short_sma >= self._previous_long_sma and 
              current_short_sma < current_long_sma):
            out.append(Signal( tick.timestamp, tick.symbol, "SELL", qty, reason=f"SMA crossover: {current_short_sma:.2f} < {current_long_sma:.2f}", strategy="SMA_CROSSOVER" ))
        
        # Store current values for next iteration
        self._previous_short_sma = current_short_sma
        self._previous_long_sma = current_long_sma
        
        return out
    
    @property
    def remaining_capital(self):
        return self._remaining_capital

    @remaining_capital.setter
    def remaining_capital(self, value: float):
        self._remaining_capital = value

