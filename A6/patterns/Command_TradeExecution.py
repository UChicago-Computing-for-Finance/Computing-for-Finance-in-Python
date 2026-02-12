from abc import ABC, abstractmethod
from datetime import datetime

class Command(ABC):
    """Abstract command interface."""
    
    @abstractmethod
    def execute(self) -> bool:
        """Execute the command. Returns True if successful."""
        pass
    
    @abstractmethod
    def undo(self) -> bool:
        """Undo the command. Returns True if successful."""
        pass


class ExecuteOrderCommand(Command):
    """Command to execute a trade order."""
    
    def __init__(self, engine, timestamp: datetime, symbol: str, action: str, price: float, quantity: int):
        self.engine = engine
        self.timestamp = timestamp
        self.symbol = symbol
        self.action = action
        self.price = price
        self.quantity = quantity
        self.executed = False
        
        # Store state for undo
        self.before_cash = 0
        self.before_position_qty = 0
        self.before_avg_cost = 0.0
    
    def execute(self) -> bool:
        """Execute the trade."""
        if self.executed:
            return False  # Already executed
        
        # Store current state for undo
        self.before_cash = self.engine.cash
        
        if self.symbol in self.engine.positions:
            pos = self.engine.positions[self.symbol]
            self.before_position_qty = pos.quantity
            self.before_avg_cost = pos.avg_cost
        else:
            self.before_position_qty = 0
            self.before_avg_cost = 0.0
        
        # Execute trade
        cost = self.price * self.quantity
        
        if self.action == 'BUY':
            if self.engine.cash >= cost:
                self.engine.cash -= cost
                if self.symbol not in self.engine.positions:
                    from engine import Position
                    self.engine.positions[self.symbol] = Position(symbol=self.symbol)
                self.engine.positions[self.symbol].update_position('BUY', self.price, self.quantity)
                self._add_trade()
                self.executed = True
                return True
        
        elif self.action == 'SELL':
            if self.symbol in self.engine.positions:
                pos = self.engine.positions[self.symbol]
                if pos.quantity >= self.quantity:
                    self.engine.cash += cost
                    pos.update_position('SELL', self.price, self.quantity)
                    self._add_trade()
                    self.executed = True
                    return True
        
        return False
    
    def undo(self) -> bool:
        """Undo the trade by reversing all changes."""
        if not self.executed:
            return False
        
        # Reverse the cash change
        if self.action == 'BUY':
            self.engine.cash += self.price * self.quantity
        elif self.action == 'SELL':
            self.engine.cash -= self.price * self.quantity
        
        # Reverse the position
        if self.action == 'BUY':
            if self.symbol in self.engine.positions:
                pos = self.engine.positions[self.symbol]
                pos.quantity -= self.quantity
                if pos.quantity == 0:
                    pos.total_cost = 0
                    pos.avg_cost = 0
                else:
                    pos.total_cost = pos.avg_cost * pos.quantity
        elif self.action == 'SELL':
            if self.symbol in self.engine.positions:
                pos = self.engine.positions[self.symbol]
                pos.quantity += self.quantity
                pos.total_cost = pos.avg_cost * pos.quantity
        
        # Remove the trade from history
        if self.engine.trades:
            self.engine.trades.pop()
        
        self.executed = False
        return True
    
    def _add_trade(self):
        """Add trade to engine's trade history."""
        from engine import Trade
        cost = self.price * self.quantity if self.action == 'BUY' else -self.price * self.quantity
        trade = Trade(
            timestamp=self.timestamp,
            symbol=self.symbol,
            action=self.action,
            price=self.price,
            quantity=self.quantity,
            cost=cost
        )
        self.engine.trades.append(trade)


class UndoOrderCommand(Command):
    """Command wrapper that undoes another command."""
    
    def __init__(self, execute_command: ExecuteOrderCommand):
        self.execute_command = execute_command
    
    def execute(self) -> bool:
        """Undo the wrapped command."""
        return self.execute_command.undo()
    
    def undo(self) -> bool:
        """Redo by executing the wrapped command."""
        return self.execute_command.execute()


class CommandInvoker:
    """Manages command history for undo/redo functionality."""
    
    def __init__(self):
        self.history = []
        self.current_index = -1
    
    def execute_command(self, command: Command) -> bool:
        """Execute a command and add to history."""
        # Remove any commands after current_index (branched undo scenario)
        if self.current_index < len(self.history) - 1:
            self.history = self.history[:self.current_index + 1]
        
        if command.execute():
            self.history.append(command)
            self.current_index += 1
            return True
        return False
    
    def undo(self) -> bool:
        """Undo the last executed command."""
        if self.current_index >= 0:
            command = self.history[self.current_index]
            if command.undo():
                self.current_index -= 1
                return True
        return False
    
    def redo(self) -> bool:
        """Redo the next command in history."""
        if self.current_index < len(self.history) - 1:
            next_command = self.history[self.current_index + 1]
            if next_command.execute():
                self.current_index += 1
                return True
        return False
    
    def get_history_length(self) -> int:
        """Get total number of commands in history."""
        return len(self.history)