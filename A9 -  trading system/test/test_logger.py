import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logger import Logger
from order import Order, OrderState
from risk_engine import RiskEngine


def test_logger():
    """Test suite for Logger singleton."""
    
    # Clean up any existing test file
    test_file = "test_events.json"
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print("=" * 60)
    print("Logger Test Suite")
    print("=" * 60)
    
    # Test 1: Singleton pattern
    print("\n--- TEST 1: Singleton Pattern ---")
    logger1 = Logger(test_file)
    logger2 = Logger(test_file)
    assert logger1 is logger2, "Logger should be a singleton"
    print("✓ Singleton pattern verified")
    
    # Clear events for fresh start
    logger1.clear()
    
    # Test 2: Log order creation
    print("\n--- TEST 2: Order Creation Logging ---")
    order1 = Order(symbol="AAPL", qty=100, side="1")
    logger1.log_order_created(order1)
    events = logger1.get_events('ORDER_CREATED')
    assert len(events) == 1, "Should have 1 ORDER_CREATED event"
    assert events[0]['data']['symbol'] == 'AAPL'
    print("✓ Order creation logged")
    
    # Test 3: Log state changes
    print("\n--- TEST 3: State Change Logging ---")
    old_state = order1.state
    order1.transition(OrderState.ACKED)
    logger1.log_state_change(order1, old_state, order1.state)
    
    old_state = order1.state
    order1.transition(OrderState.FILLED)
    logger1.log_state_change(order1, old_state, order1.state)
    
    state_events = logger1.get_events('STATE_CHANGE')
    assert len(state_events) == 2, "Should have 2 STATE_CHANGE events"
    print("✓ State changes logged")
    
    # Test 4: Log risk events
    print("\n--- TEST 4: Risk Event Logging ---")
    risk = RiskEngine(max_order_size=1000, max_position=2000)
    
    order2 = Order(symbol="MSFT", qty=500, side="1")
    logger1.log_order_created(order2)
    
    # Successful risk check
    if risk.check(order2):
        logger1.log_risk_check(order2, True)
    print("✓ Risk approval logged")
    
    # Failed risk check (order too large)
    order3 = Order(symbol="GOOGL", qty=1500, side="1")
    logger1.log_order_created(order3)
    if not risk.check(order3):
        logger1.log_risk_check(order3, False, "Order size exceeds limit")
    
    risk_events = logger1.get_events('RISK_REJECTED')
    assert len(risk_events) == 1, "Should have 1 RISK_REJECTED event"
    print("✓ Risk rejection logged")
    
    # Test 5: Log position updates
    print("\n--- TEST 5: Position Update Logging ---")
    old_pos = risk.get_position("MSFT")
    order2.transition(OrderState.ACKED)
    order2.transition(OrderState.FILLED)
    risk.update_position(order2)
    new_pos = risk.get_position("MSFT")
    logger1.log_position_update("MSFT", old_pos, new_pos, order2)
    
    pos_events = logger1.get_events('POSITION_UPDATE')
    assert len(pos_events) == 1, "Should have 1 POSITION_UPDATE event"
    print("✓ Position update logged")
    
    # Test 6: Statistics
    print("\n--- TEST 6: Event Statistics ---")
    stats = logger1.get_stats()
    print(f"Total events: {stats['total_events']}")
    print("Events by type:")
    for event_type, count in stats['by_type'].items():
        print(f"  {event_type}: {count}")
    
    assert stats['total_events'] > 0, "Should have logged events"
    print("✓ Statistics generated")
    
    # Test 7: Persistence
    print("\n--- TEST 7: Event Persistence ---")
    total_events = len(logger1.events)
    
    # Create a new logger instance (should load existing events due to singleton)
    logger3 = Logger(test_file)
    assert len(logger3.events) == total_events, "Events should persist"
    print("✓ Events persisted to disk")
    
    # Test 8: Complete workflow
    print("\n--- TEST 8: Complete Trading Workflow ---")
    logger1.clear()  # Start fresh
    
    # Create order
    order4 = Order(symbol="TSLA", qty=200, side="1")
    logger1.log_order_created(order4)
    
    # Risk check (should pass)
    if risk.check(order4):
        logger1.log_risk_check(order4, True)
        
        # Acknowledge
        old_state = order4.state
        order4.transition(OrderState.ACKED)
        logger1.log_state_change(order4, old_state, order4.state)
        
        # Fill
        old_state = order4.state
        order4.transition(OrderState.FILLED)
        logger1.log_state_change(order4, old_state, order4.state)
        
        # Update position
        old_pos = risk.get_position("TSLA")
        risk.update_position(order4)
        new_pos = risk.get_position("TSLA")
        logger1.log_position_update("TSLA", old_pos, new_pos, order4)
    
    workflow_stats = logger1.get_stats()
    assert workflow_stats['by_type']['ORDER_CREATED'] >= 1
    assert workflow_stats['by_type']['RISK_APPROVED'] >= 1
    assert workflow_stats['by_type']['STATE_CHANGE'] >= 2
    assert workflow_stats['by_type']['POSITION_UPDATE'] >= 1
    print("✓ Complete workflow logged")
    
    print("\n" + "=" * 60)
    print("Final Statistics:")
    print("=" * 60)
    final_stats = logger1.get_stats()
    print(f"Total events: {final_stats['total_events']}")
    for event_type, count in sorted(final_stats['by_type'].items()):
        print(f"  {event_type}: {count}")
    
    print("\n✓ All logger tests passed!")
    
    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)
    print(f"\nCleaned up test file: {test_file}")


if __name__ == "__main__":
    test_logger()
