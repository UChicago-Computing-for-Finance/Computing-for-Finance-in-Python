import csv
from datetime import datetime
import pandas as pd

class PriceLoader:
    def __init__(self):
        self._market_data: pd.Series = pd.Series()

    def load_data(self, csv_path: str) -> pd.Series:
            data = []
            timestamps = []
            with open(csv_path, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)  # skip header
                for row in reader:
                    timestamp = datetime.fromisoformat(row[0])
                    price = float(row[2])
                    timestamps.append(timestamp)
                    data.append(price)
            series = pd.Series(data=data, index=pd.to_datetime(timestamps))
            return series