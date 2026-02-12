import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from risk_engine import RiskEngine
from order import Order, OrderState


def test_risk_engine():
    """Comprehensive test suite for RiskEngine."""
    
    print("=" * 60)
    print("Risk Engine Test Suite")
    print("=" * 60)
    
    # Initialize risk engine
    risk = RiskEngine(max_order_size=1000, max_position=2000)
    
    print("\n--- TEST 1: Valid order within limits ---")
    order1 = Order(symbol="AAPL", qty=500, side="1")  # Buy 500
    if risk.check(order1):
        print(f"✓ Order APPROVED: Buy {order1.qty} {order1.symbol}")
        order1.transition(OrderState.ACKED)
        order1.transition(OrderState.FILLED)
        risk.update_position(order1)
    else:
        print(f"✗ Order REJECTED")
    
    print("\n--- TEST 2: Another valid order for same symbol ---")
    order2 = Order(symbol="AAPL", qty=800, side="1")  # Buy 800 more
    if risk.check(order2):
        print(f"✓ Order APPROVED: Buy {order2.qty} {order2.symbol}")
        order2.transition(OrderState.ACKED)
        order2.transition(OrderState.FILLED)
        risk.update_position(order2)
    else:
        print(f"✗ Order REJECTED")
    
    print("\n--- TEST 3: Order exceeds max_order_size ---")
    order3 = Order(symbol="MSFT", qty=1500, side="1")  # Too large
    if risk.check(order3):
        print(f"✓ Order APPROVED: Buy {order3.qty} {order3.symbol}")
        order3.transition(OrderState.ACKED)
    else:
        print(f"✗ Order REJECTED")
        order3.transition(OrderState.REJECTED)
    
    print("\n--- TEST 4: Order would exceed max_position ---")
    order4 = Order(symbol="AAPL", qty=800, side="1")  # Would make position 2100
    if risk.check(order4):
        print(f"✓ Order APPROVED: Buy {order4.qty} {order4.symbol}")
        order4.transition(OrderState.ACKED)
    else:
        print(f"✗ Order REJECTED")
        order4.transition(OrderState.REJECTED)
    
    print("\n--- TEST 5: Valid sell order (reduces position) ---")
    order5 = Order(symbol="AAPL", qty=600, side="2")  # Sell 600
    if risk.check(order5):
        print(f"✓ Order APPROVED: Sell {order5.qty} {order5.symbol}")
        order5.transition(OrderState.ACKED)
        order5.transition(OrderState.FILLED)
        risk.update_position(order5)
    else:
        print(f"✗ Order REJECTED")
    
    print("\n--- TEST 6: Different symbol ---")
    order6 = Order(symbol="GOOGL", qty=300, side="1")  # Buy 300 GOOGL
    if risk.check(order6):
        print(f"✓ Order APPROVED: Buy {order6.qty} {order6.symbol}")
        order6.transition(OrderState.ACKED)
        order6.transition(OrderState.FILLED)
        risk.update_position(order6)
    else:
        print(f"✗ Order REJECTED")
    
    print("\n--- TEST 7: Sell creating short position ---")
    order7 = Order(symbol="TSLA", qty=1000, side="2")  # Sell 1000 (go short)
    if risk.check(order7):
        print(f"✓ Order APPROVED: Sell {order7.qty} {order7.symbol}")
        order7.transition(OrderState.ACKED)
        order7.transition(OrderState.FILLED)
        risk.update_position(order7)
    else:
        print(f"✗ Order REJECTED")
    
    print("\n--- TEST 8: Order would exceed short position limit ---")
    order8 = Order(symbol="TSLA", qty=1500, side="2")  # Would make position -2500
    if risk.check(order8):
        print(f"✓ Order APPROVED: Sell {order8.qty} {order8.symbol}")
        order8.transition(OrderState.ACKED)
    else:
        print(f"✗ Order REJECTED")
        order8.transition(OrderState.REJECTED)
    
    print("\n" + "=" * 60)
    print("Final Positions:")
    print("=" * 60)
    for symbol, position in risk.get_all_positions().items():
        print(f"{symbol}: {position:+d}")
    
    # Assertions for automated testing
    assert risk.get_position("AAPL") == 700, f"Expected AAPL position 700, got {risk.get_position('AAPL')}"
    assert risk.get_position("GOOGL") == 300, f"Expected GOOGL position 300, got {risk.get_position('GOOGL')}"
    assert risk.get_position("TSLA") == -1000, f"Expected TSLA position -1000, got {risk.get_position('TSLA')}"
    
    print("\n✓ All assertions passed!")


if __name__ == "__main__":
    test_risk_engine()
