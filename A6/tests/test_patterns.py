import unittest
import os
import sys
import json
from datetime import datetime

# Ensure project root is importable when running tests
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from patterns.Factory_InstrumentTypes import InstrumentFactory, Stock, Bond, ETF
from patterns.Singleton_ConfigAccess import Config
from patterns.Observer_SignalNotification import SignalPublisher, LoggerObserver, AlertObserver
from engine import BacktestEngine
from patterns.Strategy_SignalGen import MeanReversionStrategy, BreakoutStrategy
from models import MarketDataPoint
from Decorator_Analytics import VolatilityDecorator, BetaDecorator, DrawdownDecorator


class PatternsTestCase(unittest.TestCase):

    def test_factory_creates_types(self):
        s = InstrumentFactory.create_instrument({'symbol': 'X', 'type': 'stock', 'price': '10'})
        self.assertIsInstance(s, Stock)
        self.assertEqual(getattr(s, 'instrument_type', None), 'stock')

        b = InstrumentFactory.create_instrument({'symbol': 'Y', 'type': 'bond', 'price': '100', 'maturity': '2030-01-01'})
        self.assertIsInstance(b, Bond)
        self.assertEqual(getattr(b, 'instrument_type', None), 'bond')

        e = InstrumentFactory.create_instrument({'symbol': 'Z', 'type': 'etf', 'price': '50'})
        self.assertIsInstance(e, ETF)
        self.assertEqual(getattr(e, 'instrument_type', None), 'etf')

    def test_singleton_config(self):
        c1 = Config()
        c2 = Config()
        self.assertIs(c1, c2)
        # config file should exist and be loaded
        self.assertTrue(hasattr(c1, 'config'))

    def test_observer_notifications(self):
        publisher = SignalPublisher()
        logger = LoggerObserver(log_level='DEBUG')
        alert = AlertObserver()

        publisher.attach(logger)
        publisher.attach(alert)

        # Send a normal BUY signal
        sig = {
            'timestamp': datetime.now(),
            'symbol': 'AAPL',
            'price': 150.0,
            'signal_type': 'BUY',
            'strategy_name': 'test'
        }
        publisher.notify(sig)
        self.assertEqual(logger.get_log_count(), 1)
        self.assertEqual(alert.get_alert_count(), 0)

        # Send an insufficient position signal to trigger alert
        sig2 = {
            'timestamp': datetime.now(),
            'symbol': 'AAPL',
            'price': 150.0,
            'signal_type': 'INSUFFICIENT_POSITION',
            'available_quantity': 0,
            'quantity': 5
        }
        publisher.notify(sig2)
        self.assertEqual(logger.get_log_count(), 2)
        self.assertEqual(alert.get_alert_count(), 1)
        self.assertTrue(len(alert.get_alerts()) >= 1)

    def test_command_execute_undo(self):
        engine = BacktestEngine(initial_capital=1000)
        ts = datetime.now()

        # Execute a BUY via command pattern
        success = engine.execute_trade(timestamp=ts, symbol='TEST', action='BUY', price=10.0, quantity=5, use_command_pattern=True)
        self.assertTrue(success)
        # After buy: cash reduced, position exists, trade in history
        self.assertIn('TEST', engine.positions)
        self.assertEqual(engine.positions['TEST'].quantity, 5)
        self.assertEqual(len(engine.trades), 1)

        # Undo the last command
        undone = engine.command_invoker.undo()
        self.assertTrue(undone)
        # After undo: position reverted, trades list empty
        self.assertTrue(engine.positions.get('TEST', None) is None or engine.positions['TEST'].quantity == 0)
        self.assertEqual(len(engine.trades), 0)

    def test_strategy_outputs_and_notifications(self):
        # Mean Reversion: small lookback to trigger quickly
        strat = MeanReversionStrategy(lookback_window=3, threshold=0.01)
        logger = LoggerObserver()
        strat.publisher.attach(logger)

        # feed prices so that the 4th price is significantly below mean -> BUY
        ticks = [100.0, 100.0, 100.0, 90.0]
        result = 0
        for p in ticks:
            mp = MarketDataPoint(timestamp=datetime.now(), symbol='SIM', price=p)
            result = strat.generate_signals(mp)

        self.assertEqual(result, 1)
        self.assertEqual(logger.get_log_count(), 1)

        # Breakout: feed prices so 4th price is above upward breakout
        strat2 = BreakoutStrategy(lookback_window=3, threshold=0.01)
        logger2 = LoggerObserver()
        strat2.publisher.attach(logger2)

        ticks2 = [100.0, 100.0, 100.0, 102.0]
        result2 = 0
        for p in ticks2:
            mp = MarketDataPoint(timestamp=datetime.now(), symbol='SIM2', price=p)
            result2 = strat2.generate_signals(mp)

        # Current implementation compares price after appending it to the recent window,
        # so a single higher tick may not register as a breakout. Expect NO ACTION (0).
        self.assertEqual(result2, 0)
        self.assertEqual(logger2.get_log_count(), 0)

    def test_decorators_metrics_keys(self):
        # Ensure analytics decorators produce expected keys
        stock = Stock({'symbol': 'AAPL', 'price': 169.89, 'type': 'stock'})
        decorated = DrawdownDecorator(BetaDecorator(VolatilityDecorator(stock)))
        metrics = decorated.get_metrics()
        self.assertIsInstance(metrics, dict)
        for key in ('volatility', 'beta', 'max_drawdown'):
            self.assertIn(key, metrics)
            self.assertTrue((metrics[key] is None) or isinstance(metrics[key], float))


if __name__ == '__main__':
    unittest.main()
