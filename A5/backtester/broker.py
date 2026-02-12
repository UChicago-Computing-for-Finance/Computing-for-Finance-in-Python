class Broker:
    def __init__(self, cash: float = 1_000_000):
        self.cash = cash
        self.position = 0

    def market_order(self, side: str, qty: int, price: float):

        if qty <= 0:
            raise ValueError(f"Quantity must be positive: {qty}")
        if price <= 0:
            raise ValueError(f"Price must be positive: {price}")

        if side == "BUY":
            total_cost = qty * price
            if self.cash < total_cost:
                raise ValueError(f"Insufficient cash to buy {qty} units at {price:.2f}: available {self.cash:.2f}")
            self.cash -= total_cost
            self.position += qty

        elif side == "SELL":
            if self.position < qty:
                raise ValueError(f"Insufficient position to sell {qty} units: available position is {self.position}")
            self.cash += qty * price
            self.position -= qty

        else:
            raise ValueError(f"Unknown order side: {side}")
