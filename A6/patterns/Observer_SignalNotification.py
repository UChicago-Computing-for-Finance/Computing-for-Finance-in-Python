from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List
import json

# Problem: Notify external modules when signals are generated.
# Expectations:
# Implement SignalPublisher with .attach(observer) and .notify(signal).
# Define Observer.update(signal: dict).
# Implement:
# LoggerObserver: logs signals
# AlertObserver: alerts on large trades
# Demonstrate dynamic observer registration and notification.

class Observer(ABC):
    """Abstract observer interface for signal notifications."""
    
    @abstractmethod
    def update(self, signal: Dict):
        """
        Handle signal notification.
        
        Args:
            signal: Dictionary containing signal information
        """
        pass


class SignalPublisher:
    """Publishes signals to registered observers."""
    
    def __init__(self):
        self.observers: List[Observer] = []
    
    def attach(self, observer: Observer):
        """Attach an observer to receive notifications."""
        if observer not in self.observers:
            self.observers.append(observer)
            print(f"Observer {observer.__class__.__name__} attached")
    
    def detach(self, observer: Observer):
        """Remove an observer from notifications."""
        if observer in self.observers:
            self.observers.remove(observer)
            print(f"Observer {observer.__class__.__name__} detached")
    
    def notify(self, signal: Dict):
        """Notify all registered observers about a signal."""
        for observer in self.observers:
            observer.update(signal)


class LoggerObserver(Observer):
    """Observer that logs all signals to console."""
    
    def __init__(self, log_level: str = 'INFO'):
        self.log_level = log_level
        self.log_count = 0
    
    def update(self, signal: Dict):
        """Log the signal to console."""
        self.log_count += 1
        
        log_entry = {
            'timestamp': signal.get('timestamp'),
            'symbol': signal.get('symbol'),
            'signal_type': signal.get('signal_type'),
            'strategy': signal.get('strategy_name'),
            'price': signal.get('price'),
            'action': signal.get('action', 'NONE')
        }
        
        print(f"[{self.log_level}] Signal #{self.log_count}: "
              f"{signal.get('signal_type', 'NONE')} signal for {signal.get('symbol')} "
              f"at ${signal.get('price', 0):.2f} using {signal.get('strategy_name', 'Unknown')} "
              f"- Action: {signal.get('action', 'NONE')}")
    
    def get_log_count(self) -> int:
        """Return total number of signals logged."""
        return self.log_count


class AlertObserver(Observer):
    """Observer that alerts on insufficient positions to sell or insufficient funds."""
    
    def __init__(self, alert_threshold: float = 0):
        self.alert_threshold = alert_threshold
        self.alert_count = 0
        self.alerts = []
    
    def update(self, signal: Dict):
        """Alert when there is insufficient position to sell or insufficient funds."""
        signal_type = signal.get('signal_type')
        
        # Alert on insufficient position to sell
        if signal_type == 'INSUFFICIENT_POSITION':
            self.alert_count += 1
            alert_msg = (f"ALERT #{self.alert_count}: INSUFFICIENT POSITION\n"
                        f"   Cannot sell {signal.get('quantity', 1)} share(s) of {signal.get('symbol')} "
                        f"at ${signal.get('price', 0):.2f}\n"
                        f"   Available: {signal.get('available_quantity', 0)} share(s)")
            self.alerts.append(alert_msg)
            print(alert_msg)
    
    def get_alert_count(self) -> int:
        """Return total number of alerts triggered."""
        return self.alert_count
    
    def get_alerts(self) -> List[str]:
        """Return all alert messages."""
        return self.alerts