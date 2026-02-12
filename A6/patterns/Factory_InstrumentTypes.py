import csv

# Implement InstrumentFactory.create_instrument(data: dict) -> Instrument.
# Support at least three instrument types with appropriate attributes.
# Demonstrate instantiation from instruments.csv.

class Instrument:
    def __init__(self, data: dict):
        self.data = data
        self.symbol = data.get('symbol')
        self.price = float(data.get('price', 0)) if data.get('price') else 0
        self.sector = data.get('sector')
        self.issuer = data.get('issuer')


class InstrumentFactory:

    @staticmethod
    def create_instrument(data: dict) -> Instrument:

        instrument_type = data.get('type', '').lower()

        if instrument_type == 'stock':
            return Stock(data)
        elif instrument_type == 'bond':
            return Bond(data)
        elif instrument_type == 'etf':
            return ETF(data)
        else:
            raise ValueError(f"Invalid instrument type: {instrument_type}")

    @staticmethod
    def load_from_csv(filepath: str):

        instruments = []
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    instrument = InstrumentFactory.create_instrument(row)
                    instruments.append(instrument)
                except Exception as e:
                    print(f"Error creating instrument from {row}: {e}")
        return instruments

# Instruments

class Stock(Instrument):
    def __init__(self, data: dict):
        super().__init__(data)
        self.instrument_type = 'stock'

class Bond(Instrument):
    def __init__(self, data: dict):
        super().__init__(data)
        self.instrument_type = 'bond'
        self.maturity = data.get('maturity')
        
class ETF(Instrument):
    def __init__(self, data: dict):
        super().__init__(data)
        self.instrument_type = 'etf'
