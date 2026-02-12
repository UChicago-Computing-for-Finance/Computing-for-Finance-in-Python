import pandas as pd

def test_signals_length(strategy, prices):
    '''
    Test: signals length and type (shape/format)
    '''
    sig = strategy.signals(prices)
    assert len(sig) == len(prices)
    assert isinstance(sig, pd.Series)
    assert all(v in {-1, 0, 1} for v in sig)


def test_no_signals_on_insufficient_history(strategy, prices_short):
    '''
    Test: no signals when insufficient history
    '''
    sig = strategy.signals(prices_short)
    assert all(v == 0 for v in sig)

def test_buy_signals_on_price_jump(strategy_with_short_window, prices_with_jump):
    '''
    Test: buy signals on price jump
    '''
    sig = strategy_with_short_window.signals(prices_with_jump)
    # Expect buy signals after the jump (index > 150)
    assert any(sig[150:] == 1)
    # No sell signals in a rising market
    assert all(v != -1 for v in sig)

def test_buy_signals_on_price_dump(strategy_with_short_window, prices_with_dump):
    '''
    Test: buy signals on price dump
    '''
    sig = strategy_with_short_window.signals(prices_with_dump)
    # Expect buy signals after the dump (index > 150)
    assert any(sig[150:] == -1)
    # No sell signals in a rising market
    assert all(v != 1 for v in sig)

def test_no_signals_on_constant_prices(strategy, prices_constant):
    '''
    Test: no signals on constant prices (zero volatility)
    '''
    sig = strategy.signals(prices_constant)
    assert all(v == 0 for v in sig)

def test_index_alignment_and_nan_handling(strategy, prices_with_NaNs):
    '''
    Test: index alignment and NaN handling
    '''
    sig = strategy.signals(prices_with_NaNs)
    assert len(sig) == len(prices_with_NaNs)
    # Ensure no signals are generated where prices are NaN
    assert all(sig[prices_with_NaNs.isna()] == 0)  
    # Ensure signals are only -1, 0, or 1
    assert all(v in {-1, 0, 1} for v in sig if pd.notna(v))
    
def test_signals_with_non_series_input(strategy):
    '''
    Test: signals with non-Series input (e.g., list or numpy array)
    '''
    prices_list = list(range(100, 120))
    sig = strategy.signals(prices_list)
    assert len(sig) == len(prices_list)
    assert isinstance(sig, pd.Series)
    assert all(v in {-1, 0, 1} for v in sig)

def test_value_equality_computed(strategy_with_short_window):
    """Compute expected signals with pandas and assert equality.

    This test demonstrates the canonical way to compute expected outputs in tests:
    - compute returns via pct_change
    - compute rolling std over prior returns (shifted)
    - compare today's return to k * rolling_std to build expected signals
    """

    prices = pd.Series([100.0, 100.5, 101.0, 100.8, 101.5, 103.0, 102.0])
    sig = strategy_with_short_window.signals(prices)

    # compute expected signals using the same rules as the strategy
    returns = prices.pct_change()
    rolling_std = returns.shift(1).rolling(window=strategy_with_short_window.window, min_periods=strategy_with_short_window.window).std(ddof=0)

    expected = pd.Series(0, index=prices.index)
    buy_mask = (returns > (strategy_with_short_window.k * rolling_std))
    sell_mask = (returns < (-strategy_with_short_window.k * rolling_std))
    expected[buy_mask.fillna(False)] = 1
    expected[sell_mask.fillna(False)] = -1

    pd.testing.assert_series_equal(sig, expected)


def test_empty_series(strategy):
        """Edge: empty price series should return empty signals series."""
        prices = pd.Series([], dtype=float)
        sig = strategy.signals(prices)
        assert isinstance(sig, pd.Series)
        assert len(sig) == 0

def test_single_element_series(strategy):
    """Edge: single-element series should return a single zero signal."""
    prices = pd.Series([100.0])
    sig = strategy.signals(prices)
    assert len(sig) == 1
    assert sig.iloc[0] == 0

def test_with_zero_prices(strategy):
    """Edge: price series containing zero should not raise; signals defined.

    We avoid divide-by-zero in pct_change implicitly; ensure function returns a series.
    """
    prices = pd.Series([100.0, 0.0, 0.0, 101.0, 102.0])
    sig = strategy.signals(prices)
    assert isinstance(sig, pd.Series)
    assert len(sig) == len(prices)
