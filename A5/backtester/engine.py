import pandas as pd
import numpy as np

class Backtester:
    def __init__(self, strategy, broker):
        self.strategy = strategy
        self.broker = broker

    def run(self, prices: pd.Series):

        if prices is None or len(prices) == 0:
            raise ValueError("Prices series is empty")

        signals = self.strategy.signals(prices)

        if len(signals) != len(prices):
            raise ValueError("Prices and signals series must have the same length")

        shifted_signals = signals.shift(1)
        # Skip the first signal (since it will always be NaN after shift)
        for timestamp, signal in list(shifted_signals.items())[1:]:
            if signal not in [-1, 0, 1]:
                raise ValueError(f"Invalid signal: {signal}")
            if pd.isna(signal):
                continue
            if signal == 1:
                self.broker.market_order("BUY", 1, prices[timestamp])
            elif signal == -1:
                self.broker.market_order("SELL", 1, prices[timestamp])
            else:
                pass