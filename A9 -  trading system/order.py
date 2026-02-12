# order.py
from enum import Enum, auto
from logger import Logger

class OrderState(Enum):
    NEW = auto()
    ACKED = auto()
    FILLED = auto()
    CANCELED = auto()
    REJECTED = auto()

class Order:
    def __init__(self, symbol, qty, side, use_logger=True):
        self.symbol = symbol
        self.qty = qty
        self.side = side
        self.state = OrderState.NEW
        self.use_logger = use_logger
        
        # Log order creation
        if self.use_logger:
            logger = Logger()
            logger.log_order_created(self)

    def transition(self, new_state):
        allowed = {
            OrderState.NEW: {OrderState.ACKED, OrderState.REJECTED},
            OrderState.ACKED: {OrderState.FILLED, OrderState.CANCELED},

            # since no outgoing transitions
            OrderState.FILLED: set(),
            OrderState.CANCELED: set(),
            OrderState.REJECTED: set(),
        }

        if new_state in allowed[self.state]:
            old_state = self.state
            self.state = new_state
            
            # Log state transition
            if self.use_logger:
                logger = Logger()
                logger.log_state_change(self, old_state, new_state)
        else:
            raise ValueError(f"Bad transition: {self.state} -> {new_state}")