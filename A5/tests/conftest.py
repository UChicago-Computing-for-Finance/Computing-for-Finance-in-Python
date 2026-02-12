import numpy as np, pandas as pd, pytest
from backtester.strategy import VolatilityBreakoutStrategy
from backtester.broker import Broker

@pytest.fixture
def strategy():
    return VolatilityBreakoutStrategy()

@pytest.fixture
def strategy_with_short_window():
    return VolatilityBreakoutStrategy(window=3, k=0.5)

@pytest.fixture
def broker():
    return Broker(cash=1_000)


@pytest.fixture
def prices():
    # deterministic rising series
    return pd.Series(np.linspace(100, 120, 200))

@pytest.fixture
def prices_with_jump():
    # deterministic rising series with a jump
    prices = np.linspace(100, 120, 200)
    prices[150:] += 10
    return pd.Series(prices)

@pytest.fixture
def prices_with_dump():
    # deterministic rising series with a dump
    prices = np.linspace(120, 100, 200)
    prices[150:] -= 10
    return pd.Series(prices)

@pytest.fixture
def prices_short():
    # short series to test edge cases
    return pd.Series([100, 101, 102, 103, 104])

@pytest.fixture
def prices_constant():
    # constant price series to test zero volatility
    return pd.Series([100] * 200)

@pytest.fixture
def prices_with_NaNs():
    # price series with NaN values
    prices = np.linspace(100, 120, 200)
    prices[::10] = np.nan
    return pd.Series(prices)