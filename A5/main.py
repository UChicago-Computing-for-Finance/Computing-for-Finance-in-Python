from backtester.engine import Backtester
from backtester.broker import Broker
from backtester.strategy import VolatilityBreakoutStrategy
from backtester.price_loader import PriceLoader
import pandas as pd

price_loader = PriceLoader()
pd_series = price_loader.load_data("backtester/data/market_data_1000.csv") 

broker = Broker()
strategy = VolatilityBreakoutStrategy()

backtest = Backtester(strategy, broker)

backtest.run(pd_series)

print(broker.cash)
print(broker.position)