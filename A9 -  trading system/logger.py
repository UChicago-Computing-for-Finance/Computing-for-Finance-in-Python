from datetime import datetime
import json
import os


class Logger:
    """
    Singleton event logger for recording system activity.
    
    Records all order creation, state transitions, and risk events
    for replay and analysis.
    """
    
    _instance = None
    
    def __new__(cls, path="events.json"):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, path="events.json"):
        """
        Initialize the logger.
        
        Args:
            path: Path to the JSON file where events will be saved
        """
        # Only initialize once (singleton pattern)
        if self._initialized:
            return
        
        self.path = path
        self.events = []
        self._initialized = True
        
        # Load existing events if file exists
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    self.events = json.load(f)
                print(f"Loaded {len(self.events)} existing events from {path}")
            except (json.JSONDecodeError, IOError):
                print(f"Could not load events from {path}, starting fresh")
                self.events = []
    
    def log(self, event_type, data):
        """
        Log an event.
        
        Args:
            event_type: Type of event (e.g., 'ORDER_CREATED', 'STATE_CHANGE', 'RISK_CHECK')
            data: Dictionary containing event data
        """
        event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'data': data
        }
        self.events.append(event)
        
        # Also print to console for real-time monitoring
        print(f"[{event['timestamp']}] {event_type}: {data}")
        
        # Auto-save after each event for durability
        self.save()
    
    def log_order_created(self, order):
        """Log order creation event."""
        self.log('ORDER_CREATED', {
            'symbol': order.symbol,
            'qty': order.qty,
            'side': order.side,
            'state': order.state.name
        })
    
    def log_state_change(self, order, old_state, new_state):
        """Log order state transition event."""
        self.log('STATE_CHANGE', {
            'symbol': order.symbol,
            'qty': order.qty,
            'side': order.side,
            'old_state': old_state.name,
            'new_state': new_state.name
        })
    
    def log_risk_check(self, order, passed, reason=None):
        """Log risk check event."""
        data = {
            'symbol': order.symbol,
            'qty': order.qty,
            'side': order.side,
            'passed': passed
        }
        if reason:
            data['reason'] = reason
        
        event_type = 'RISK_APPROVED' if passed else 'RISK_REJECTED'
        self.log(event_type, data)
    
    def log_position_update(self, symbol, old_position, new_position, order):
        """Log position update event."""
        self.log('POSITION_UPDATE', {
            'symbol': symbol,
            'old_position': old_position,
            'new_position': new_position,
            'order_qty': order.qty,
            'order_side': order.side
        })
    
    def save(self):
        """Save all events to JSON file."""
        try:
            with open(self.path, 'w') as f:
                json.dump(self.events, f, indent=2)
        except IOError as e:
            print(f"Error saving events to {self.path}: {e}")
    
    def get_events(self, event_type=None):
        """
        Retrieve events, optionally filtered by type.
        
        Args:
            event_type: Optional filter for event type
            
        Returns:
            List of events
        """
        if event_type:
            return [e for e in self.events if e['event_type'] == event_type]
        return self.events.copy()
    
    def clear(self):
        """Clear all events (useful for testing)."""
        self.events = []
        self.save()
    
    def get_stats(self):
        """Get statistics about logged events."""
        stats = {
            'total_events': len(self.events),
            'by_type': {}
        }
        
        for event in self.events:
            event_type = event['event_type']
            stats['by_type'][event_type] = stats['by_type'].get(event_type, 0) + 1
        
        return stats
