from models import MarketDataPoint, Signal, Strategy
from typing import List, Optional
from statistics import pstdev


class VolatilityBreakoutStrategy(Strategy):
    def __init__(self, symbol: str, capital: int, k: float = 0.5):
        self.ticks = []
        self.prices = []
        self.k = k
        super().__init__(symbol, capital)

        # position state: 0 = flat, 1 = long
        self.position: int = 0

    
    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float = None) -> List[Signal]:
        self.ticks.append(tick)
        self.prices.append(tick.price)

        if len(self.prices) < 2:
            return []  # need yesterday's close to compute today's return

        prev_close = self.prices[-2]
        if prev_close == 0:
            return []  # guard divide-by-zero

        # today's simple return
        daily_return = (tick.price - prev_close) / prev_close

        # build list of historical daily returns EXCLUDING today's return
        returns = []
        # iterate up to the previous day (stop before last price)
        for i in range(1, len(self.prices) - 1):
            p0 = self.prices[i - 1]
            p1 = self.prices[i]
            if p0 == 0:
                returns.append(0.0)
            else:
                returns.append((p1 - p0) / p0)

        # need at least 20 prior returns for rolling-20 std
        if len(returns) < 20:
            return []

        rolling_20_std = pstdev(returns[-20:])  # population std over the 20 prior returns

        #volume = min(self._remaining_capital, max_order_vol)
        #qty = int(volume / tick.price)
        qty = 1 # for this assignment, just trade 1 share at a time

        # ENTRY: buy when today's return exceeds rolling 20-day std and we are flat
        if daily_return > rolling_20_std and self.position == 0:
            self.position = 1
            return [Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="BUY", quantity=qty, strategy='VolatilityBreakoutStrategy', reason=f'Return={daily_return:.4f} > Rolling20Std={rolling_20_std:.4f}')]

        # EXIT: sell when today's return is a large negative move and we're long
        if self.position == 1 and daily_return < -rolling_20_std:
            self.position = 0
            return [Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="SELL", quantity=qty, strategy='VolatilityBreakoutStrategy', reason=f'Return={daily_return:.4f} < -Rolling20Std={-rolling_20_std:.4f}')]

        return []
    