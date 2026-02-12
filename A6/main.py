import importlib
import engine

importlib.reload(engine)

from patterns.Strategy_SignalGen import MeanReversionStrategy
from patterns.Strategy_SignalGen import BreakoutStrategy

# Initialize engine
engine = engine.BacktestEngine(initial_capital=100000)

# Load market data
print("Loading market data...")
df = engine.load_market_data()

# Get unique symbols
symbols = df['symbol'].unique()
print(f"Found symbols: {symbols}")

# Initialize strategies
mean_reversion = MeanReversionStrategy()
mean_reversion.load_params()

# ------------------------------------------------------------
# strategy.set_strategy(mean_reversion)
# ------------------------------------------------------------

breakout = BreakoutStrategy()
breakout.load_params()


engine.backtest_strategy(mean_reversion, 'AAPL', df)


# Overall summary
print(f"\n{'='*80}")
print("BACKTEST SUMMARY")
print(f"{'='*80}")
print(f"Total trades executed: {len(engine.trades)}")
print(f"Final cash: ${engine.cash:,.2f}")
print(f"Open positions: {sum(1 for p in engine.positions.values() if p.quantity > 0)}")