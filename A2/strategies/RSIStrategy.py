from models import MarketDataPoint, Signal, Strategy
from typing import List, Optional


class RSIStrategy(Strategy):

    def __init__(self, symbol: str, capital: int, period: int = 14, overbought: int = 70, oversold: int = 30):
        self.ticks = []
        self.prices = []
        self.period = period
        self.overbought = overbought
        self.oversold = oversold
        super().__init__(symbol, capital)

    def calculate_rsi(self) -> Optional[float]:
        if len(self.prices) < self.period + 1:
            return None  # Not enough data to calculate RSI

        gains = []
        losses = []

        for i in range(1, len(self.prices)):
            change = self.prices[i] - self.prices[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(-change)

        avg_gain = sum(gains[-self.period:]) / self.period
        avg_loss = sum(losses[-self.period:]) / self.period

        if avg_loss == 0:
            return 100.0  # Prevent division by zero; implies strong upward trend

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float = None) -> List[Signal]:
        self.ticks.append(tick)
        self.prices.append(tick.price)

        rsi = self.calculate_rsi()
        if rsi is None:
            return []  # Not enough data to generate signals

        #volume = min(self._remaining_capital, max_order_vol)
        #qty = int(volume / tick.price)
        qty = 1 # for this assignment, just trade 1 share at a time
        
        if rsi > self.overbought:
            return [Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="SELL", quantity=qty, strategy='RSIStrategy', reason=f'RSI={rsi:.2f} > {self.overbought}')]
        elif rsi < self.oversold:
            return [Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="BUY", quantity=qty, strategy='RSIStrategy', reason=f'RSI={rsi:.2f} < {self.oversold}')]

        return []