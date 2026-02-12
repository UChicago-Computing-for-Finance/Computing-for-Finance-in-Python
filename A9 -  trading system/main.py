from fix_parser import FixParser
from order import Order, OrderState
from risk_engine import RiskEngine
from logger import Logger

fix = FixParser()
risk = RiskEngine()
log = Logger()

# Price (44=150.50) for limit order, or change to Market order (40=1)
raw = "8=FIX.4.2|35=D|55=AAPL|54=1|38=500|40=2|44=150.50|10=128"
msg = fix.parse(raw)

order = Order(msg["55"], int(msg["38"]), msg["54"])

# Check risk before acknowledging
if risk.check(order):
    order.transition(OrderState.ACKED)
    log.log_state_change(order, OrderState.NEW, OrderState.ACKED)
    
    # Simulate fill
    order.transition(OrderState.FILLED)
    log.log_state_change(order, OrderState.ACKED, OrderState.FILLED)
    
    # Update position after fill
    risk.update_position(order)
    
    print(f"✓ Order filled: {order.symbol} {order.qty} shares")
else:
    order.transition(OrderState.REJECTED)
    log.log_state_change(order, OrderState.NEW, OrderState.REJECTED)
    print(f"✗ Order rejected: {order.symbol} {order.qty} shares")

# Events are auto-saved by the singleton logger
print(f"\nFinal position: {risk.get_position('AAPL')} shares")