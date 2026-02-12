
from abc import ABC, abstractmethod

# Problem: Model portfolios as trees of positions and sub-portfolios.
# Expectations:
# Define abstract PortfolioComponent with .get_value() and .get_positions().
# Implement:
# Position: leaf node
# PortfolioGroup: composite node
# Demonstrate recursive aggregation from portfolio_structure.json.

class PortfolioComponent(ABC):
    """Abstract base class for portfolio components."""
    
    @abstractmethod
    def get_value(self) -> float:
        """Calculate and return the total value of this component."""
        pass
    
    @abstractmethod
    def get_positions(self) -> list:
        """Return a list of all positions in this component."""
        pass


class Position(PortfolioComponent):
    """Leaf node representing a single position in the portfolio."""
    
    def __init__(self, symbol: str, quantity: int, price: float):
        self.symbol = symbol
        self.quantity = quantity
        self.price = price
    
    def get_value(self) -> float:
        """Returns the value of this single position."""
        return self.quantity * self.price
    
    def get_positions(self) -> list:
        """Returns a list containing this single position."""
        return [{'symbol': self.symbol, 'quantity': self.quantity, 'price': self.price}]
    
    def __repr__(self):
        return f"Position({self.symbol}, qty={self.quantity}, price=${self.price})"


class PortfolioGroup(PortfolioComponent):
    """Composite node representing a group of positions or sub-portfolios."""
    
    def __init__(self, name: str):
        self.name = name
        self.components = []
    
    def add(self, component: PortfolioComponent):
        """Add a child component to this portfolio group."""
        self.components.append(component)
    
    def get_value(self) -> float:
        """Recursively calculate the total value by summing all child components."""
        return sum(component.get_value() for component in self.components)
    
    def get_positions(self) -> list:
        """Recursively collect all positions from child components."""
        positions = []
        for component in self.components:
            positions.extend(component.get_positions())
        return positions
    
    def __repr__(self):
        return f"PortfolioGroup(name={self.name}, components={len(self.components)})"


def build_portfolio_from_json(filepath: str) -> PortfolioGroup:
    """Demonstrate recursive aggregation from portfolio_structure.json."""
    import json
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    def build_recursive(portfolio_data: dict) -> PortfolioGroup:
        """Recursively build portfolio structure from JSON data."""
        portfolio = PortfolioGroup(portfolio_data.get('name', 'Unnamed Portfolio'))
        
        # Add direct positions (leaf nodes)
        for pos_data in portfolio_data.get('positions', []):
            position = Position(
                symbol=pos_data['symbol'],
                quantity=pos_data['quantity'],
                price=pos_data['price']
            )
            portfolio.add(position)
        
        # Add sub-portfolios (composite nodes)
        for sub_port_data in portfolio_data.get('sub_portfolios', []):
            sub_portfolio = build_recursive(sub_port_data)
            portfolio.add(sub_portfolio)
        
        return portfolio
    
    return build_recursive(data)