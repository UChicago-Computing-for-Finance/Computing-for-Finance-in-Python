# engine.py
import csv
import logging
import random
from datetime import datetime
from typing import List, Dict
import pandas as pd
import json
import numbers
from models import *

# Strategies -------------------
from strategies.NaiveMovingAverageStrategy import NaiveMovingAverageStrategy
from strategies.WindowedMovingAverageStrategy import WindowedMovingAverageStrategy

# from strategies.MACDStrategy import MACDStrategy
# from strategies.VolatilityBreakoutStrategy import VolatilityBreakoutStrategy
# from strategies.RSIStrategy import RSIStrategy
# from strategies.BenchmarkStrategy import BenchmarkStrategy
# ------------------------------

from pathlib import Path

log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "engine.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    filename=str(log_file),   # write to file instead of stderr
    filemode="a",             # 'a' append, use 'w' to overwrite each run
)
logger = logging.getLogger(__name__)


class ExecutionEngine:
    """
    Core engine that ties everything together:
    - Buffers market data
    - Runs strategies to generate signals
    - Converts signals to orders
    - Updates positions when orders are "executed"
    """

    def __init__(self, failure_rate: float = 0.0, initial_capital: float = 100000.0, short_positions: bool = False):
        # Containers
        self._market_data: List[MarketDataPoint] = []  # all ticks
        self._signals: List[Signal] = []               # all signals
        self._orders: List[Order] = []                 # all orders
        self._positions: Dict[str, Dict] = {}          # {symbol: {"quantity": int, "avg_price": float}}        
        self._failure_rate = failure_rate              # For simulating execution failures
        self.short_positions = short_positions          # Allow short selling or not
        # Capital and performance tracking
        self._initial_capital = initial_capital
        self._current_capital = initial_capital
        
        # Per-strategy-class tracking
        self._strategy_positions: Dict[str, Dict[str, Dict[str, Dict]]] = {}  # {strategy_class: {symbol: {timestamp: position_data}}}
        self._strategy_signals: Dict[str, Dict[str, Dict[str, Dict]]] = {}    # {strategy_class: {symbol: {timestamp: signal_data}}}
        self._strategy_orders: Dict[str, Dict[str, Dict[str, Dict]]] = {}     # {strategy_class: {symbol: {timestamp: order_data}}}
        

    def load_data(self, csv_path: str):
        """Read market data from a CSV file and store as MarketDataPoint list."""
        with open(csv_path, "r", newline="") as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for row in reader:
                # Parse fields with defensive checks
                timestamp = datetime.fromisoformat(row[0])
                symbol = row[1]
                price = float(row[2])

                daily_volume = None
                if len(row) > 3 and row[3]:
                    raw_vol = row[3].strip()
                    # sometimes CSVs contain float-like strings (e.g. '1441600.0')
                    # MarketDataPoint.daily_volume is Optional[float], parse to float
                    try:
                        # remove commas if present and convert
                        daily_volume = float(raw_vol.replace(',',''))
                    except ValueError:
                        logger.warning(f"Could not parse daily_volume '{raw_vol}' for {symbol} at {row[0]}; setting None")

                tick = MarketDataPoint(
                    timestamp=timestamp,
                    symbol=symbol,
                    price=price,
                    daily_volume=daily_volume
                )
                self._market_data.append(tick)
        logger.info(f"Loaded {len(self._market_data)} ticks from {csv_path}")


    def initialize_strategies(self, strategies: List[Strategy]):
        """Initialize strategy tracking and allocate capital equally among strategies."""
        if not strategies:
            raise ValueError("Cannot initialize with empty strategy list")
        
        # Get unique strategy classes
        strategy_classes = set()
        for strategy in strategies:
            strategy_class = strategy.__class__.__name__
            strategy_classes.add(strategy_class)
            
            # Initialize strategy class dictionaries if not exists
            if strategy_class not in self._strategy_positions:
                self._strategy_positions[strategy_class] = {}
                self._strategy_signals[strategy_class] = {}
                self._strategy_orders[strategy_class] = {}
            
            # Initialize symbol dictionaries if not exists
            symbol = strategy._symbol
            if symbol not in self._strategy_positions[strategy_class]:
                self._strategy_positions[strategy_class][symbol] = {}
                self._strategy_signals[strategy_class][symbol] = {}
                self._strategy_orders[strategy_class][symbol] = {}
            
            strategy.remaining_capital = self._initial_capital
            logger.info(f"Allocated ${self._initial_capital:.2f} to {strategy_class}_{symbol}")
        
    def run(self, strategies: List[Strategy], data_path: str = "data/market_data.csv"):
        """
        Run the backtest by processing data symbol-by-symbol for each strategy.
        """
        
        self.load_data(data_path)
        logger.info(f"Starting backtest with {len(self._market_data)} ticks")

        self.initialize_strategies(strategies)
        self._market_data.sort(key=lambda tick: tick.timestamp)
        
        for tick in self._market_data:     
            for strategy in strategies:
                if strategy._symbol != tick.symbol:
                    continue
                try:
                    max_order_vol = tick.daily_volume * 0.075 if tick.daily_volume is not None else None

                    signals = strategy.generate_signals(tick, max_order_vol)
                    strategy_class = strategy.__class__.__name__
                    symbol = strategy._symbol
                    timestamp = tick.timestamp.isoformat()
                    
                    for signal in signals:
                        try:
                            order = Order(signal.symbol, signal.quantity, tick.price, "PENDING")
                            self._execute_order_direct(order, signal.side, strategy)                                
                            # Store in global lists
                            self._signals.append(signal) 
                            self._orders.append(order)
                            
                            # Store in per-strategy class dictionaries with timestamp
                            self._strategy_signals[strategy_class][symbol][timestamp] = {
                                "signal": signal,
                                "side": signal.side,
                                "quantity": signal.quantity,
                                "price": tick.price
                            }
                            self._strategy_orders[strategy_class][symbol][timestamp] = {
                                "order": order,
                                "symbol": order.symbol,
                                "quantity": order.quantity,
                                "price": order.price,
                                "status": order.status
                            }

                        except (OrderError, ExecutionError) as e:
                            logger.error(f"Order failed for {signal.symbol}: {e}")
                            # Still store failed order for analysis
                            if 'order' in locals():
                                order.status = "FAILED"
                                self._orders.append(order)
                                self._strategy_orders[strategy_class][symbol][timestamp] = {
                                    "order": order,
                                    "symbol": order.symbol,
                                    "quantity": order.quantity,
                                    "price": order.price,
                                    "status": order.status
                                }
                        except Exception as e:
                            logger.error(f"Unexpected error processing signal: {e}")
                            
                except Exception as e:
                    logger.error(f"Strategy {strategy.__class__.__name__} failed on {tick.symbol}: {e}")
                    continue
                # After processing signals for this strategy at this tick, snapshot current position and cash
                # Only record if there's a change from the previous snapshot
                try:
                    strategy_class = strategy.__class__.__name__
                    symbol = strategy._symbol
                    timestamp = tick.timestamp.isoformat()
                    
                    # Get current position from the strategy's position tracking
                    current_pos = strategy._current_position
                    
                    # Create current snapshot data
                    current_snapshot = {
                        "qty": current_pos['quantity'],
                        "avg_price": current_pos['avg_price'],
                        "remaining_cash": strategy.remaining_capital
                    }
                    
                    # Get the last recorded snapshot for comparison
                    symbol_positions = self._strategy_positions[strategy_class][symbol]
                    if symbol_positions:
                        # Get the most recent entry (dict is ordered by insertion)
                        last_timestamp = list(symbol_positions.keys())[-1]
                        last_snapshot = symbol_positions[last_timestamp]
                        
                        # Only store if there's a change
                        if (current_snapshot["qty"] != last_snapshot["qty"] or 
                            current_snapshot["avg_price"] != last_snapshot["avg_price"] or 
                            current_snapshot["remaining_cash"] != last_snapshot["remaining_cash"]):
                            self._strategy_positions[strategy_class][symbol][timestamp] = current_snapshot
                    else:
                        # First entry for this strategy-symbol combination, always store
                        self._strategy_positions[strategy_class][symbol][timestamp] = current_snapshot
                        
                except Exception as e:
                    logger.debug(f"Failed to snapshot position for {strategy.__class__.__name__}_{strategy._symbol}: {e}")
                    
        # Save per-strategy data to JSON for analysis
        try:
            self.save_strategy_data()
        except Exception as e:
            logger.error(f"Failed to save strategy data: {e}")

        logger.info(f"Backtest completed. Generated {len(self._signals)} signals, {len(self._orders)} orders")


    def _execute_order_direct(self, order: Order, signal_side: str, strategy: Strategy):
        """Execute order immediately and update positions with capital checks."""
        
        # Simulate execution failure
        if random.random() < self._failure_rate:
            raise ExecutionError(f"Simulated execution failure for {order.symbol}")
        
        order_value = order.quantity * order.price
        strategy_capital = strategy.remaining_capital
        strategy_class = strategy.__class__.__name__
        symbol = strategy._symbol
        
        current_pos = strategy._current_position

        if signal_side == "BUY":
            # Check if strategy has enough capital for BUY orders
            if order_value > strategy_capital:
                raise ExecutionError(f"Insufficient capital for {strategy_class}_{symbol}: need ${order_value:.2f}, have ${strategy_capital:.2f}")
            
            # Deduct capital for BUY orders
            strategy.remaining_capital = strategy_capital - order_value
        
            # Update current position
            if current_pos["quantity"] == 0:
                current_pos["quantity"] = order.quantity
                current_pos["avg_price"] = order.price
            else:
                total_cost = current_pos["quantity"] * current_pos["avg_price"] + order.quantity * order.price
                current_pos["quantity"] += order.quantity
                current_pos["avg_price"] = total_cost / current_pos["quantity"]
                
        elif signal_side == "SELL":
            # Check if we have enough shares to sell
            if order.quantity > current_pos["quantity"]:
                raise ExecutionError(f"Insufficient shares to sell for {strategy_class}_{symbol}: trying to sell {order.quantity}, have {current_pos['quantity']}")

            # Update position for SELL orders
            total_cost = current_pos["quantity"] * current_pos["avg_price"]
            current_pos["quantity"] -= order.quantity
            if current_pos["quantity"] == 0:
                current_pos["avg_price"] = 0.0
            else:
                current_pos["avg_price"] = (total_cost - order.quantity * order.price) / current_pos["quantity"]
            # Add capital for SELL orders
            # Credit proceeds to strategy capital
            proceeds = order.quantity * order.price
            strategy.remaining_capital = strategy.remaining_capital + proceeds

        order.status = "FILLED"
        logger.info(f"Executed {signal_side}: {order.symbol} {order.quantity}@{order.price:.2f} | Strategy: {strategy_class}_{symbol} | Capital: ${strategy.remaining_capital:.2f} | Position : ${current_pos['quantity'] * order.price:.2f}")


    def _make_serializable(self, obj):
        """Recursively convert obj into JSON-serializable primitives.

        - dict keys are cast to str
        - numbers are converted to int when integral, otherwise float
        - datetimes are converted to ISO strings
        - other objects are stringified
        """
        # dict
        if isinstance(obj, dict):
            return {str(k): self._make_serializable(v) for k, v in obj.items()}
        # list/tuple
        if isinstance(obj, (list, tuple)):
            return [self._make_serializable(v) for v in obj]
        # primitives
        if obj is None:
            return None
        if isinstance(obj, bool):
            return obj
        if isinstance(obj, numbers.Number):
            # prefer int where possible
            try:
                f = float(obj)
                if f.is_integer():
                    return int(f)
                return float(f)
            except Exception:
                return float(obj)
        # datetime -> ISO
        if isinstance(obj, datetime):
            return obj.isoformat()
        # fallback to str
        return str(obj)

    def save_strategy_data(self, base_path: str = "logs/strategy_data", save_positions: bool = True, save_orders: bool = False, save_signals: bool = False):
        """Serialize and save strategy positions, signals, and orders to JSON files.

        Args:
            base_path: Base directory path for saving files
            save_positions: Whether to save positions data (default: True)
            save_orders: Whether to save orders data (default: False)
            save_signals: Whether to save signals data (default: False)

        The helper normalizes types (numbers, datetimes) so the output is JSON-friendly.
        """
        base_p = Path(base_path)
        base_p.mkdir(parents=True, exist_ok=True)
        
        # Save positions
        if save_positions:
            positions_path = base_p / "positions.json"
            serializable_positions = self._make_serializable(self._strategy_positions)
            with positions_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_positions, f, indent=2)
            logger.info(f"Wrote strategy positions to {positions_path}")
        
        # Save signals
        if save_signals:
            signals_path = base_p / "signals.json"
            serializable_signals = self._make_serializable(self._strategy_signals)
            with signals_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_signals, f, indent=2)
            logger.info(f"Wrote strategy signals to {signals_path}")
        
        # Save orders
        if save_orders:
            orders_path = base_p / "orders.json"
            serializable_orders = self._make_serializable(self._strategy_orders)
            with orders_path.open("w", encoding="utf-8") as f:
                json.dump(serializable_orders, f, indent=2)
            logger.info(f"Wrote strategy orders to {orders_path}")


if __name__ == "__main__":
    import sys

    # Check if a data path is provided as a command-line argument
    if len(sys.argv) > 1:
        data_path = sys.argv[1]
    else:
        data_path = "data/market_data_1000.csv"  # Default path if no argument is provided

    if len(sys.argv) > 2:
        strategy_class = sys.argv[2]
    else:
        strategy_class = 'NaiveMovingAverageStrategy'

    
    if len(sys.argv) > 1 and sys.argv[1] == "--error-demo":
        # Run just the error handling demo
        print("Error demo not implemented")
    else:
        # For each 5 strat, create One strat per ticker

        snp500_symbols = ['AAPL']
       
        strat_list = []

        num_symbols = len(snp500_symbols)
        if num_symbols == 0:
            logger.warning("No symbols found in sp500_symbols.csv; strat_list will be empty")


        if strategy_class == 'NaiveMovingAverageStrategy':
            for sym in snp500_symbols:
                v = NaiveMovingAverageStrategy(symbol=sym, capital=0)
                strat_list.append(v)
        elif strategy_class == 'WindowedMovingAverageStrategy':
            for sym in snp500_symbols:
                v = WindowedMovingAverageStrategy(symbol=sym, capital=0, window_size=20)
                strat_list.append(v)


        engine = ExecutionEngine(failure_rate=0.0, initial_capital=1_000_000.0)
        engine.run(strat_list, data_path)
        