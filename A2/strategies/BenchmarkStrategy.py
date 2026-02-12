from models import MarketDataPoint, Signal, Strategy
from typing import List

class BenchmarkStrategy(Strategy):

    def __init__(self, symbol: str, capital: int):
        self.ticks = []
        super().__init__(symbol, capital)


    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float) -> List[Signal]:
        # Simple benchmark: One buy only on the first tick

        vol = min(self._remaining_capital, max_order_vol)
        if tick.price == 0:
            return []  # guard divide-by-zero
        qty = int(vol / tick.price)
        
        if len(self.ticks) == 0:
            self.ticks.append(tick)
            return [Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="BUY", quantity=qty, strategy='BenchmarkStrategy')]
        
        return []
