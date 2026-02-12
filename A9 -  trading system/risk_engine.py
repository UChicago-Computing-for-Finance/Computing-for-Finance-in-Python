from logger import Logger


class RiskEngine:
    """
    Risk management engine that enforces position and order size limits.
    
    Tracks positions per symbol and validates orders before execution.
    """
    
    def __init__(self, max_order_size=1000, max_position=2000, use_logger=True):
        """
        Initialize the risk engine.
        
        Args:
            max_order_size: Maximum quantity allowed per order
            max_position: Maximum absolute position allowed per symbol
            use_logger: Whether to log events
        """
        self.max_order_size = max_order_size
        self.max_position = max_position
        self.positions = {}  # symbol -> net position (positive=long, negative=short)
        self.use_logger = use_logger
        self.logger = Logger() if use_logger else None
    
    def check(self, order) -> bool:
        """
        Validate an order against risk limits.
        
        Args:
            order: Order object with symbol, qty, and side attributes
                   side should be 'BUY'/'1' or 'SELL'/'2'
        
        Returns:
            True if order passes all risk checks, False otherwise
        """
        symbol = order.symbol
        qty = order.qty
        side = order.side
        
        # Check 1: Order size limit
        if qty > self.max_order_size:
            reason = f"Order size {qty} exceeds max order size {self.max_order_size}"
            print(f"RISK REJECT: {reason} for {symbol}")
            if self.use_logger:
                self.logger.log_risk_check(order, False, reason)
            return False
        
        # Get current position for this symbol (default to 0)
        current_position = self.positions.get(symbol, 0)
        
        # Calculate position change based on side
        # BUY (side='1' or 'BUY') increases position, SELL (side='2' or 'SELL') decreases
        if side in ['1', 'BUY', 'Buy']:
            position_change = qty
        elif side in ['2', 'SELL', 'Sell']:
            position_change = -qty
        else:
            reason = f"Invalid side '{side}'"
            print(f"RISK REJECT: {reason} for {symbol}")
            if self.use_logger:
                self.logger.log_risk_check(order, False, reason)
            return False
        
        # Calculate hypothetical new position
        new_position = current_position + position_change
        
        # Check 2: Position limit (absolute value)
        if abs(new_position) > self.max_position:
            reason = f"New position {new_position} would exceed max position ±{self.max_position} (current: {current_position})"
            print(f"RISK REJECT: {reason} for {symbol}")
            if self.use_logger:
                self.logger.log_risk_check(order, False, reason)
            return False
        
        # All checks passed
        if self.use_logger:
            self.logger.log_risk_check(order, True)
        return True
    
    def update_position(self, order):
        """
        Update position after an order fill.
        
        Args:
            order: Filled order with symbol, qty, and side attributes
        """
        symbol = order.symbol
        qty = order.qty
        side = order.side
        
        # Get current position (default to 0)
        current_position = self.positions.get(symbol, 0)
        
        # Calculate position change
        if side in ['1', 'BUY', 'Buy']:
            position_change = qty
        elif side in ['2', 'SELL', 'Sell']:
            position_change = -qty
        else:
            raise ValueError(f"Invalid side '{side}' in update_position")
        
        # Update position
        new_position = current_position + position_change
        self.positions[symbol] = new_position
        
        print(f"POSITION UPDATE: {symbol} {current_position:+d} -> {new_position:+d} "
              f"(filled {qty} {side})")
        
        # Log position update
        if self.use_logger:
            self.logger.log_position_update(symbol, current_position, new_position, order)
    
    def get_position(self, symbol):
        """Get current position for a symbol."""
        return self.positions.get(symbol, 0)
    
    def get_all_positions(self):
        """Get all current positions."""
        return self.positions.copy()
