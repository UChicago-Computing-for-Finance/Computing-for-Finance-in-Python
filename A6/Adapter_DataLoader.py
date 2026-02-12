import json
import xml.etree.ElementTree as ET
from datetime import datetime
from models import MarketDataPoint

# Problem: Standardize external data formats into MarketDataPoint objects.
# Expectations:
# Implement adapters:
# YahooFinanceAdapter
# BloombergXMLAdapter
# Each exposes .get_data(symbol: str) -> MarketDataPoint.
# Demonstrate ingestion from external_data_yahoo.json and external_data_bloomberg.xml.

class YahooFinanceAdapter:
    def __init__(self):
        self.data = self.load_data()

    def load_data(self):
        with open('inputs/external_data_yahoo.json', 'r') as f:
            raw_data = json.load(f)
            return [{
                'symbol': raw_data.get('ticker'),
                'price': raw_data.get('last_price'),
                'timestamp': raw_data.get('timestamp')
            }]

    def get_data(self, symbol: str) -> MarketDataPoint:
        for entry in self.data:
            if entry.get('symbol') == symbol or entry.get('ticker') == symbol:
                return MarketDataPoint(
                    timestamp=datetime.fromisoformat(entry.get('timestamp').replace('Z', '+00:00')),
                    symbol=entry.get('symbol'),
                    price=float(entry.get('price')),
                    daily_volume=None
                )


class BloombergXMLAdapter:
    def __init__(self):
        self.data = self.load_data()

    def load_data(self):
        with open('inputs/external_data_bloomberg.xml', 'r') as f:
            tree = ET.parse(f)
            root = tree.getroot()
            
            data_list = []
            
            if root.tag == 'instrument':
                # Single instrument as root
                data_list.append({
                    'symbol': root.find('symbol').text,
                    'price': root.find('price').text,
                    'timestamp': root.find('timestamp').text
                })
            else:
                # Multiple instruments as children
                for instrument in root.findall('instrument'):
                    data_list.append({
                        'symbol': instrument.find('symbol').text,
                        'price': instrument.find('price').text,
                        'timestamp': instrument.find('timestamp').text
                    })
            
            return data_list

    def get_data(self, symbol: str) -> MarketDataPoint:
        for entry in self.data:
            if entry.get('symbol') == symbol:
                return MarketDataPoint(
                    timestamp=datetime.fromisoformat(entry.get('timestamp').replace('Z', '+00:00')),
                    symbol=entry.get('symbol'),
                    price=float(entry.get('price')),
                    daily_volume=None
                )
        return None