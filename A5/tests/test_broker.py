import pytest
from backtester.broker import Broker

def test_buy_and_sell_updates_cash_and_pos(broker):
    broker.market_order("BUY", 2, 10.0)
    assert (broker.position, broker.cash) == (2, 1000 - 20.0)

def test_rejects_bad_orders(broker):
    with pytest.raises(ValueError):
        broker.market_order("BUY", 0, 10)

def test_sell_all_position(broker):
    """Test selling entire position."""
    broker.market_order("BUY", 4, 25.0)
    broker.market_order("SELL", 4, 30.0)
    assert broker.position == 0
    assert broker.cash == 1000 - 100.0 + 120.0  # 1020.0

def test_insufficient_cash_error(broker):
    """Test error when trying to buy with insufficient cash."""
    with pytest.raises(ValueError, match="Insufficient cash"):
        broker.market_order("BUY", 100, 20.0)  # Need 2000, only have 1000

def test_insufficient_position_error(broker):
    """Test error when trying to sell more than owned."""
    broker.market_order("BUY", 3, 10.0)
    with pytest.raises(ValueError, match="Insufficient position"):
        broker.market_order("SELL", 5, 15.0)  # Only have 3, trying to sell 5

def test_sell_without_position_error(broker):
    """Test error when trying to sell without any position."""
    with pytest.raises(ValueError, match="Insufficient position"):
        broker.market_order("SELL", 1, 10.0)

def test_negative_quantity_error(broker):
    """Test error for negative quantity."""
    with pytest.raises(ValueError, match="Quantity must be positive: -1"):
        broker.market_order("BUY", -1, 10.0)

def test_zero_quantity_error(broker):
    """Test error for zero quantity."""
    with pytest.raises(ValueError, match="Quantity must be positive: 0"):
        broker.market_order("BUY", 0, 10.0)

def test_invalid_side_error(broker):
    """Test error for invalid order side."""
    with pytest.raises(ValueError, match="Unknown order side: INVALID"):
        broker.market_order("INVALID", 1, 10.0)

def test_exact_cash_match(broker):
    """Test buying with exactly available cash."""
    broker.market_order("BUY", 100, 10.0)  # Exactly 1000 cash
    assert broker.position == 100
    assert broker.cash == 0.0

def test_exact_position_match(broker):
    """Test selling exactly available position."""
    broker.market_order("BUY", 5, 10.0)
    broker.market_order("SELL", 5, 15.0)  # Sell all 5
    assert broker.position == 0
    assert broker.cash == 1000 - 50.0 + 75.0

def test_fractional_prices(broker):
    """Test with fractional prices."""
    broker.market_order("BUY", 3, 33.33)
    expected_cash = 1000 - (3 * 33.33)
    assert abs(broker.cash - expected_cash) < 0.01  # Allow for floating point precision
    assert broker.position == 3

def test_large_quantities(broker):
    """Test with large quantities."""
    broker.market_order("BUY", 1000, 1.0)  # Buy 1000 shares at $1 each
    assert broker.position == 1000
    assert broker.cash == 0.0

def test_default_cash_initialization():
    """Test default cash initialization."""
    broker = Broker()
    assert broker.cash == 1_000_000
    assert broker.position == 0

def test_custom_cash_initialization():
    """Test custom cash initialization."""
    broker = Broker(cash=5000)
    assert broker.cash == 5000
    assert broker.position == 0

def test_zero_cash_initialization():
    """Test zero cash initialization."""
    broker = Broker(cash=0)
    assert broker.cash == 0
    assert broker.position == 0

def test_string_quantity_error(broker):
    """Test error for string quantity."""
    with pytest.raises(TypeError):
        broker.market_order("BUY", "1", 10.0)

def test_string_price_error(broker):
    """Test error for string price."""
    with pytest.raises(TypeError):
        broker.market_order("BUY", 1, "10.0")