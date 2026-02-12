import json

# Problem: Construct complex portfolios with nested positions and metadata.
# Expectations:
# Implement PortfolioBuilder with fluent methods:
#   add_position(symbol, quantity, price)
#   set_owner(name)
#   add_subportfolio(name, builder)
#   build() -> Portfolio
# Demonstrate building from portfolio_structure.json.

class PortfolioBuilder:
    def __init__(self):
        self.portfolio = Portfolio()

    def add_position(self, symbol: str, quantity: int, price: float):
        self.portfolio.add_position(symbol, quantity, price)
        return self

    def set_name(self, name: str):
        self.portfolio.set_name(name)
        return self

    def set_owner(self, name: str):
        if name is not None:
            self.portfolio.set_owner(name)
        return self

    def add_subportfolio(self, name: str, builder: 'PortfolioBuilder'):
        built_subportfolio = builder.build()
        self.portfolio.add_subportfolio(name, built_subportfolio)
        return self

    def build(self):
        return self.portfolio

class Director:
    def __init__(self):
        self.builder = PortfolioBuilder()

    def build_portfolio(self):

        with open('inputs/portfolio_structure.json', 'r') as f:
            data = json.load(f)

        self.builder.set_owner(data.get('owner'))
        self.builder.set_name(data.get('name'))

        for position in data.get('positions'):
            self.builder.add_position(position.get('symbol'), int(position.get('quantity')), float(position.get('price')))

        for subportfolio in data.get('sub_portfolios'):

            subportfolio_builder = PortfolioBuilder()
            subportfolio_builder.set_name(subportfolio.get('name'))

            for position in subportfolio.get('positions'):
                subportfolio_builder.add_position(position.get('symbol'), int(position.get('quantity')), float(position.get('price')))

            self.builder.add_subportfolio(subportfolio.get('name'), subportfolio_builder)

        return self.builder.build()



        
class Portfolio:
    def __init__(self):
        self.name = None
        self.owner = None
        self.positions = []
        self.subportfolios = []

    def add_position(self, symbol: str, quantity: int, price: float):
        self.positions.append({'symbol': symbol, 'quantity': quantity, 'price': price})
        
    def set_owner(self, name: str):
        self.owner = name
        
    def set_name(self, name: str):
        self.name = name

    def add_subportfolio(self, name: str, subportfolio: 'Portfolio'):
        self.subportfolios.append({'name': name, 'subportfolio': subportfolio})
        