
import csv
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from patterns.Strategy_SignalGen import Strategy, MeanReversionStrategy, BreakoutStrategy
from models import MarketDataPoint

from patterns.Command_TradeExecution import CommandInvoker


@dataclass
class Trade:
    """Represents a single trade execution."""
    timestamp: datetime
    symbol: str
    action: str  # 'BUY' or 'SELL'
    price: float
    quantity: int
    cost: float  # Positive for buy, negative for sell
    

@dataclass
class Position:
    """Tracks a position in a symbol."""
    symbol: str
    quantity: int = 0
    avg_cost: float = 0.0
    total_cost: float = 0.0
    
    def update_position(self, action: str, price: float, quantity: int = 1):
        """Update position based on trade action."""
        if action == 'BUY':
            # Calculate new average cost
            new_cost = self.total_cost + (price * quantity)
            self.quantity += quantity
            self.total_cost = new_cost
            self.avg_cost = new_cost / self.quantity if self.quantity > 0 else 0
        elif action == 'SELL':
            if self.quantity >= quantity:
                self.quantity -= quantity
                if self.quantity == 0:
                    self.total_cost = 0
                    self.avg_cost = 0
                else:
                    # Adjust average cost for remaining position
                    remaining_value = self.avg_cost * self.quantity
                    self.total_cost = remaining_value
            else:
                raise ValueError("Cannot sell more shares than owned")
    
    def get_current_value(self, current_price: float) -> float:
        """Calculate current value of position."""
        return self.quantity * current_price


class BacktestEngine:
    """Main backtesting engine that applies strategies to market data."""
    
    def __init__(self, initial_capital: float = 100000):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_curve: List[Tuple[datetime, float]] = []

        # publisher
        from patterns.Observer_SignalNotification import SignalPublisher
        self.publisher = SignalPublisher()

        # command invoker
        self.command_invoker = CommandInvoker()
    
    def load_market_data(self, filepath: str = 'inputs/market_data.csv') -> pd.DataFrame:
        """Load market data from CSV."""
        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    def get_symbol_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Extract data for a single symbol."""
        return df[df['symbol'] == symbol].sort_values('timestamp').reset_index(drop=True)
    
    def create_data_point(self, row) -> MarketDataPoint:
        """Convert DataFrame row to MarketDataPoint."""
        return MarketDataPoint(
            timestamp=row['timestamp'],
            symbol=row['symbol'],
            price=row['price']
        )
    
    def execute_trade(self, timestamp: datetime, symbol: str, action: str, price: float, quantity: int = 1, use_command_pattern: bool = False):
        """Execute a trade and update portfolio state."""

        if use_command_pattern:
            # Use Command pattern
            from patterns.Command_TradeExecution import ExecuteOrderCommand
            command = ExecuteOrderCommand(
                engine=self,
                timestamp=timestamp,
                symbol=symbol,
                action=action,
                price=price,
                quantity=quantity
            )
            return self.command_invoker.execute_command(command)


        cost = price * quantity
        
        # Create trade record
        trade = Trade(
            timestamp=timestamp,
            symbol=symbol,
            action=action,
            price=price,
            quantity=quantity,
            cost=cost if action == 'BUY' else -cost
        )
        
        if action == 'BUY':
            # Check if enough cash
            if self.cash >= cost:
                self.cash -= cost
                # Initialize or update position
                if symbol not in self.positions:
                    self.positions[symbol] = Position(symbol=symbol)
                self.positions[symbol].update_position(action, price, quantity)
                self.trades.append(trade)
                print(f"BUY:  {quantity} share(s) of {symbol} at ${price:.2f} | Cash: ${self.cash:.2f} | position: {self.positions[symbol].quantity}")
            else:
                if hasattr(self, 'publisher'):
                    signal_dict = {
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'price': price,
                        'signal_type': 'INSUFFICIENT_FUNDS',
                        'required_cash': cost,
                        'available_cash': self.cash,
                        'quantity': quantity,
                        'action': action
                    }
                    self.publisher.notify(signal_dict)
                print(f"INSUFFICIENT FUNDS: Cannot buy {symbol} at ${price:.2f} | Available: ${self.cash:.2f}")
        
        elif action == 'SELL':
            # Check if enough shares
            if symbol in self.positions and self.positions[symbol].quantity >= quantity:
                self.cash += cost
                self.positions[symbol].update_position(action, price, quantity)
                self.trades.append(trade)
                pnl = (price - self.positions[symbol].avg_cost) * quantity if symbol in self.positions else 0
                print(f"SELL: {quantity} share(s) of {symbol} at ${price:.2f} | PnL: ${pnl:.2f} | Cash: ${self.cash:.2f} | position: {self.positions[symbol].quantity}")
            else:
                # Notify observers about insufficient position
                available_quantity = self.positions.get(symbol, Position(symbol)).quantity if symbol in self.positions else 0
                if hasattr(self, 'publisher'):
                    signal_dict = {
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'price': price,
                        'signal_type': 'INSUFFICIENT_POSITION',
                        'available_quantity': available_quantity,
                        'requested_quantity': quantity,
                        'quantity': quantity,
                        'action': action
                    }
                    self.publisher.notify(signal_dict)
                print(f"INSUFFICIENT SHARES: Cannot sell {symbol} | Available: {self.positions.get(symbol, Position(symbol)).quantity if symbol in self.positions else 0}")
    
    def calculate_portfolio_value(self, df: pd.DataFrame, timestamp: datetime) -> float:
        """Calculate total portfolio value at a given timestamp."""
        portfolio_value = self.cash
        
        for symbol, position in self.positions.items():
            # Get current price from dataframe
            current_data = df[(df['symbol'] == symbol) & (df['timestamp'] == timestamp)]
            if not current_data.empty:
                current_price = current_data.iloc[0]['price']
                portfolio_value += position.get_current_value(current_price)
        
        return portfolio_value
    
    def backtest_strategy(self, strategy: Strategy, symbol: str, df: pd.DataFrame):
        """
        Backtest a strategy on a single symbol.
        Demonstrates the Strategy pattern - each strategy is interchangeable.
        """
        print(f"\n{'='*80}")
        print(f"Backtesting {strategy.__class__.__name__} on {symbol}")
        print(f"{'='*80}")
        
        # Get data for this symbol
        symbol_data = self.get_symbol_data(df, symbol)
        
        # Initialize/reset portfolio for this symbol
        # (In a multi-symbol backtest, you'd track separately)
        self.reset_positions_for_symbol(symbol)
        
        for idx, row in symbol_data.iterrows():
            # Create market data point
            tick = self.create_data_point(row)
            
            # Get signal from strategy
            signal = strategy.generate_signals(tick)
            
            # Execute trade based on signal
            if signal == 1:  # BUY signal
                self.execute_trade(tick.timestamp, symbol, 'BUY', tick.price, quantity=1)
            elif signal == -1:  # SELL signal
                self.execute_trade(tick.timestamp, symbol, 'SELL', tick.price, quantity=1)
            
            # Record equity curve periodically (every 1000 ticks)
            if idx % 1000 == 0 or idx == len(symbol_data) - 1:
                portfolio_value = self.calculate_portfolio_value(df, tick.timestamp)
                self.equity_curve.append((tick.timestamp, portfolio_value))
        
        # Print results
        self.print_results(strategy, symbol)
    
    def reset_positions_for_symbol(self, symbol: str):
        """Reset positions for a specific symbol."""
        if symbol in self.positions:
            del self.positions[symbol]
    
    def print_results(self, strategy: Strategy, symbol: str):
        """Print backtest results for a strategy-symbol combination."""
        final_portfolio_value = self.cash
        if symbol in self.positions:
            # Get last price
            position = self.positions[symbol]
            if position.quantity > 0:
                # This would need the dataframe to get final price
                final_portfolio_value += position.get_current_value(0)  # Placeholder
        
        total_return = ((final_portfolio_value - self.initial_capital) / self.initial_capital) * 100
        
        print(f"\n{'-'*80}")
        print(f"Results for {strategy.__class__.__name__} - {symbol}")
        print(f"{'-'*80}")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Final Cash: ${self.cash:,.2f}")
        print(f"Open Positions: {len([p for p in self.positions.values() if p.quantity > 0])}")
        print(f"Total Trades: {len(self.trades)}")
        print(f"Trade Breakdown:")
        buy_trades = [t for t in self.trades if t.action == 'BUY' and t.symbol == symbol]
        sell_trades = [t for t in self.trades if t.action == 'SELL' and t.symbol == symbol]
        print(f"  BUY trades: {len(buy_trades)}")
        print(f"  SELL trades: {len(sell_trades)}")
        print(f"{'-'*80}\n")


