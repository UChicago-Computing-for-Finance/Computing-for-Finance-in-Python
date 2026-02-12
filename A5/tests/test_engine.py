from unittest.mock import MagicMock
from backtester.engine import Backtester
from backtester.broker import Broker
import pytest
import pandas as pd
import numpy as np

# example
def test_engine_uses_tminus1_signal(prices, broker, strategy, monkeypatch):
    # Force exactly one buy at t=10 by controlling signals
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = prices*0
    fake_strategy.signals.return_value.iloc[9] = 1  # triggers buy at t=10
    bt = Backtester(fake_strategy, broker)
    eq = bt.run(prices)
    assert broker.position == 1
    assert broker.cash == 1000 - float(prices.iloc[10])


def test_invalid_signal(prices, broker, strategy, monkeypatch):

    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([-1, 0, 1, 2, 3])
    bt = Backtester(fake_strategy, broker)

    with pytest.raises(ValueError):
        bt.run(prices)

def test_engine_handles_single_price():
    """Test that engine handles single price point."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([1], index=[0])
    broker = Broker(cash=1000)
    bt = Backtester(fake_strategy, broker)
    
    # Should not raise an error and broker state should remain unchanged
    bt.run(pd.Series([100], index=[0]))
    assert broker.cash == 1000
    assert broker.position == 0

def test_engine_handles_all_nan_signals():
    """Test that engine handles all NaN signals."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([np.nan, np.nan, np.nan])
    broker = Broker(cash=1000)
    bt = Backtester(fake_strategy, broker)

    with pytest.raises(ValueError):
        bt.run(pd.Series([100, 101, 102]))

def test_engine_handles_insufficient_cash():
    """Test that engine handles broker's insufficient cash error."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([0, 1, 0, 0])  # Buy at index 1
    broker = Broker(cash=50)  # Not enough cash for expensive stock
    bt = Backtester(fake_strategy, broker)
    
    with pytest.raises(ValueError, match="Insufficient cash"):
        bt.run(pd.Series([100, 200, 300, 400]))  # Price 200 > cash 50

def test_engine_handles_multiple_buys_sells():
    """Test that engine handles multiple buy and sell signals correctly."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([0, 1, 0, 1, -1, -1, 0])
    broker = Broker(cash=1000)
    bt = Backtester(fake_strategy, broker)
    
    bt.run(pd.Series([100, 101, 102, 103, 104, 105, 106]))
    # Should have bought 2, sold 2, so position = 0
    assert broker.position == 0
    assert broker.cash == 1000 - 101 - 103 + 104 + 105

def test_engine_handles_negative_prices():
    """Test that engine handles negative prices (should raise error from broker)."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([0, 1, 0])
    broker = Broker(cash=1000)
    bt = Backtester(fake_strategy, broker)
    
    with pytest.raises(ValueError, match="Price must be positive: -102"):
        bt.run(pd.Series([-100, -50, -102]))  # Negative price should trigger broker error

def test_engine_handles_zero_prices():
    """Test that engine handles zero prices (should raise error from broker)."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([1, 0, 0])
    broker = Broker(cash=1000)
    bt = Backtester(fake_strategy, broker)
    
    with pytest.raises(ValueError, match="Price must be positive"):
        bt.run(pd.Series([100, 0, 102]))  # Zero price should trigger broker error

def test_engine_handles_none_strategy():
    """Test that engine handles None strategy."""
    broker = Broker(cash=1000)
    bt = Backtester(None, broker)
    
    with pytest.raises(AttributeError):
        bt.run(pd.Series([100, 101, 102]))

def test_engine_handles_none_broker():
    """Test that engine handles None broker."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([0, 1, 0])
    bt = Backtester(fake_strategy, None)
    
    with pytest.raises(AttributeError):
        bt.run(pd.Series([100, 101, 102]))

def test_engine_handles_empty_prices():
    """Test that engine handles None broker."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([0, 1, 0])
    bt = Backtester(fake_strategy, None)
    
    with pytest.raises(ValueError):
        bt.run(pd.Series([]))

def test_engine_handles_insufficient_position():
    """Test that engine handles broker's insufficient position error."""
    fake_strategy = MagicMock()
    fake_strategy.signals.return_value = pd.Series([1, -1,-1, 0])  # Buy 1, sell 2
    broker = Broker(cash=1000)
    bt = Backtester(fake_strategy, broker)
    
    with pytest.raises(ValueError, match="Insufficient position to sell 1 units: available position is 0"):
        bt.run(pd.Series([100, 101, 102, 103]))