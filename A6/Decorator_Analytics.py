from typing import List, Optional, Any, Dict
import math
import pandas as pd

MARKET_DATA_CSV = 'inputs/market_data.csv'


class InstrumentDecorator:
    """Base decorator that forwards attribute access to the wrapped instrument.

    Important: We do NOT modify the wrapped instrument. Decorators add a
    `get_metrics()` method that augments any existing metrics.
    """

    def __init__(self, instrument: Any):
        self._instrument = instrument

    def __getattr__(self, name: str):
        # forward everything else to wrapped instrument
        return getattr(self._instrument, name)

    def get_metrics(self) -> Dict[str, Any]:
        # If the wrapped object already exposes metrics, start from that.
        if hasattr(self._instrument, 'get_metrics'):
            base = self._instrument.get_metrics() or {}
        else:
            base = {}
        return dict(base)


class VolatilityDecorator(InstrumentDecorator):
    """Compute simple volatility from `inputs/market_data.csv` for the instrument's symbol.

    This is intentionally simple: we compute the standard deviation of
    simple returns (pct change) over the available data and return the
    (unannualized) std as `volatility`.
    """

    def __init__(self, instrument: Any, window: int = None):
        super().__init__(instrument)
        self.window = window

    def _load_returns(self):
        try:
            df = pd.read_csv(MARKET_DATA_CSV, parse_dates=['timestamp'])
        except Exception:
            return None
        sym = getattr(self._instrument, 'symbol', None)
        if sym is None:
            return None
        s = df.loc[df['symbol'] == sym].sort_values('timestamp')
        if s.empty:
            return None
        returns = s['price'].pct_change().dropna()
        if self.window:
            returns = returns.tail(self.window)
        return returns

    def get_metrics(self):
        metrics = super().get_metrics()
        returns = self._load_returns()
        if returns is None or returns.empty:
            metrics['volatility'] = None
            return metrics
        vol = float(returns.std())
        metrics['volatility'] = vol
        return metrics


class BetaDecorator(InstrumentDecorator):
    """Compute beta vs a market proxy (default 'SPY').

    Beta = cov(r_i, r_m) / var(r_m). Uses simple returns from data file.
    If market symbol not found or insufficient data, returns None.
    """

    def __init__(self, instrument: Any, market_symbol: str = 'SPY', window: int = None):
        super().__init__(instrument)
        self.market_symbol = market_symbol
        self.window = window

    def _load_pair_returns(self):
        try:
            df = pd.read_csv(MARKET_DATA_CSV, parse_dates=['timestamp'])
        except Exception:
            return None, None
        sym = getattr(self._instrument, 'symbol', None)
        if sym is None:
            return None, None
        s = df.loc[df['symbol'] == sym].sort_values('timestamp')
        m = df.loc[df['symbol'] == self.market_symbol].sort_values('timestamp')
        if s.empty or m.empty:
            return None, None
        # align on timestamp by inner join
        merged = pd.merge(s[['timestamp', 'price']], m[['timestamp', 'price']], on='timestamp', suffixes=('_s', '_m'))
        if merged.empty:
            return None, None
        r_s = merged['price_s'].pct_change().dropna()
        r_m = merged['price_m'].pct_change().dropna()
        if self.window:
            r_s = r_s.tail(self.window)
            r_m = r_m.tail(self.window)
        # keep equal lengths
        n = min(len(r_s), len(r_m))
        return r_s.tail(n), r_m.tail(n)

    def get_metrics(self):
        metrics = super().get_metrics()
        r_s, r_m = self._load_pair_returns()
        if r_s is None or r_m is None or len(r_s) < 2 or len(r_m) < 2:
            metrics['beta'] = None
            return metrics
        cov = float(r_s.cov(r_m))
        var_m = float(r_m.var())
        beta = cov / var_m if var_m and not math.isclose(var_m, 0.0) else None
        metrics['beta'] = beta
        return metrics


class DrawdownDecorator(InstrumentDecorator):
    """Compute maximum drawdown from historical prices.

    Returns `max_drawdown` as a positive fraction (e.g., 0.05 == 5%).
    """

    def __init__(self, instrument: Any):
        super().__init__(instrument)

    def _load_prices(self):
        try:
            df = pd.read_csv(MARKET_DATA_CSV, parse_dates=['timestamp'])
        except Exception:
            return None
        sym = getattr(self._instrument, 'symbol', None)
        if sym is None:
            return None
        s = df.loc[df['symbol'] == sym].sort_values('timestamp')
        if s.empty:
            return None
        return s['price']

    def get_metrics(self):
        metrics = super().get_metrics()
        prices = self._load_prices()
        if prices is None or prices.empty:
            metrics['max_drawdown'] = None
            return metrics
        running_max = prices.cummax()
        drawdowns = (running_max - prices) / running_max
        max_dd = float(drawdowns.max())
        metrics['max_drawdown'] = max_dd
        return metrics
