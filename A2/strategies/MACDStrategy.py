from models import MarketDataPoint, Signal, Strategy
from typing import List, Optional

class MACDStrategy(Strategy):
    """
    Buy when MACD line crosses above the signal line.
    EMA fast (default 12), EMA slow (default 26), signal EMA (default 9).
    """
    def __init__(self, symbol: str, capital: int, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        # initialize base class first (sets remaining capital)
        super().__init__(symbol, capital)

        self.ticks = []
        self.prices = []
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

        # EMA state
        self.ema_fast: Optional[float] = None
        self.ema_slow: Optional[float] = None

        # MACD / signal history and state
        self.macd_history: List[float] = []
        self.signal_ema: Optional[float] = None

        # previous values used for crossover detection
        self.prev_macd: Optional[float] = None
        self.prev_signal: Optional[float] = None

        # track simplified position state: 0 = flat, 1 = long
        self.position: int = 0

    def _alpha(self, period: int) -> float:
        return 2.0 / (period + 1)

    def generate_signals(self, tick: MarketDataPoint, max_order_vol: float) -> List[Signal]:
        self.ticks.append(tick)
        self.prices.append(tick.price)
        out: List[Signal] = []

        price = tick.price
        # update fast EMA
        if self.ema_fast is None:
            if len(self.prices) >= self.fast_period:
                self.ema_fast = sum(self.prices[-self.fast_period:]) / self.fast_period
        else:
            a_fast = self._alpha(self.fast_period)
            self.ema_fast = price * a_fast + self.ema_fast * (1 - a_fast)

        # update slow EMA
        if self.ema_slow is None:
            if len(self.prices) >= self.slow_period:
                self.ema_slow = sum(self.prices[-self.slow_period:]) / self.slow_period
        else:
            a_slow = self._alpha(self.slow_period)
            self.ema_slow = price * a_slow + self.ema_slow * (1 - a_slow)

        # need both EMAs to compute MACD
        if self.ema_fast is None or self.ema_slow is None:
            return out

        macd = self.ema_fast - self.ema_slow
        self.macd_history.append(macd)

        # update signal EMA (EMA of MACD)
        if self.signal_ema is None:
            if len(self.macd_history) >= self.signal_period:
                self.signal_ema = sum(self.macd_history[-self.signal_period:]) / self.signal_period
        else:
            a_signal = self._alpha(self.signal_period)
            self.signal_ema = macd * a_signal + self.signal_ema * (1 - a_signal)

        # need previous macd and signal to detect a crossover
        if self.prev_macd is None or self.prev_signal is None or self.signal_ema is None:
            self.prev_macd = macd
            self.prev_signal = self.signal_ema
            return out

        # BUY when MACD crosses above signal line
        qty = 1  # for this assignment trade 1 share

        # ENTRY: buy when MACD crosses above signal and we are flat
        if self.prev_macd <= self.prev_signal and macd > self.signal_ema and self.position == 0:
            self.position = 1
            out.append(Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="BUY", quantity=qty, strategy='MACDStrategy', reason=f"MACD crossover: {macd:.6f} > {self.signal_ema:.6f}"))

        # EXIT: sell when MACD crosses below signal and we are long
        elif self.prev_macd >= self.prev_signal and macd < self.signal_ema and self.position == 1:
            self.position = 0
            out.append(Signal(timestamp=tick.timestamp, symbol=tick.symbol, side="SELL", quantity=qty, strategy='MACDStrategy', reason=f"MACD crossunder: {macd:.6f} < {self.signal_ema:.6f}"))

        # store previous values for next tick
        self.prev_macd = macd
        self.prev_signal = self.signal_ema

        return out