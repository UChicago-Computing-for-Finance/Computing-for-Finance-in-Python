"""
Simple data loader for test files.
Uses the same CSV loading logic as the engine.
"""

import csv
import os
from datetime import datetime
from typing import List
from models import MarketDataPoint


def load_market_data_from_csv(file_path: str) -> List[MarketDataPoint]:
    """
    Load market data from CSV file using engine's logic.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of MarketDataPoint objects
    """
    data = []
    
    with open(file_path, "r", newline="") as file:
        reader = csv.reader(file)
        next(reader)  # skip header
        for row in reader:
            # Parse using same logic as engine.py
            timestamp = datetime.fromisoformat(row[0])
            symbol = row[1]
            price = float(row[2])
            
            # Handle optional daily_volume (if present)
            daily_volume = None
            if len(row) > 3 and row[3]:
                try:
                    daily_volume = float(row[3].replace(',', ''))
                except ValueError:
                    daily_volume = None
            
            tick = MarketDataPoint(
                timestamp=timestamp,
                symbol=symbol,
                price=price,
                daily_volume=daily_volume
            )
            data.append(tick)
    
    return data


def get_data_file_path(size: str) -> str:
    """
    Get path to market data file for given size.
    
    Args:
        size: '1000', '10000', or '100000'
        
    Returns:
        Absolute path to the data file
    """
    return os.path.join('data', f'market_data_{size}.csv')


def load_test_data(size: str) -> List[MarketDataPoint]:
    """
    Load test data for specified size.
    
    Args:
        size: '1000', '10000', or '100000'
        
    Returns:
        List of MarketDataPoint objects from real data file
    """
    file_path = get_data_file_path(size)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    return load_market_data_from_csv(file_path)