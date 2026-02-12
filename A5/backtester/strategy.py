import pandas as pd
import numpy as np


class VolatilityBreakoutStrategy:
    """Simple volatility breakout strategy suitable for the tests.

    - Constructor has sensible defaults so tests can call `VolatilityBreakoutStrategy()`.
    - `signals(prices: pd.Series) -> pd.Series` returns a series of the same length as
      `prices` with values in {-1, 0, 1} where:
        1 => buy signal (today's return > rolling x-day std of prior returns)
       -1 => sell signal (today's return < - rolling x-day std of prior returns)
        0 => no action / insufficient history

    Implementation notes:
    - Uses population std (ddof=0) to match the original `pstdev` behaviour.
    - Rolling std is computed on returns up to the previous day (shifted).
    """

    def __init__(self, window: int = 20, k: float = 1.0):
        self.window = int(window)
        self.k = float(k)

    def signals(self, prices: pd.Series) -> pd.Series:
        """Return a pd.Series of signals (-1, 0, 1) aligned with `prices`.

        For index t the strategy compares today's simple return r_t to the rolling std
        of returns over the previous `window` days (i.e., using r_{t-window} .. r_{t-1}).
        """
        if not isinstance(prices, pd.Series):
            prices = pd.Series(prices)

        # prepare empty signals (0 default)
        signals = pd.Series(0, index=prices.index)

        if len(prices) < 2:
            return signals

        # simple returns r_t = (p_t - p_{t-1}) / p_{t-1}
        returns = prices.pct_change()

        # rolling std of prior returns: shift returns so rolling window does not include r_t
        rolling_std = returns.shift(1).rolling(window=self.window, min_periods=self.window).std(ddof=0)

        # generate signals where we have a valid rolling std and today's return
        valid = rolling_std.notna() & returns.notna()

        buy_mask = valid & (returns > (self.k * rolling_std))
        sell_mask = valid & (returns < (-self.k * rolling_std))

        signals[buy_mask] = 1
        signals[sell_mask] = -1

        return signals
