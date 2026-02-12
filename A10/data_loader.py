"""
Data loader module for loading and validating multi-ticker OHLCV data.
"""
import pandas as pd


def load_and_validate_data(csv_path='market_data_multi.csv'):
    """
    Load and validate multi-ticker market data from CSV.
    
    Args:
        csv_path: Path to the CSV file containing market data
        
    Returns:
        pandas.DataFrame: Validated market data
        
    Raises:
        ValueError: If validation fails
    """
    # Load data
    df = pd.read_csv(csv_path)
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Validate no missing timestamps or prices
    required_columns = ['timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume']
    for col in required_columns:
        if df[col].isnull().any():
            raise ValueError(f"Missing values found in column: {col}")
    
    # Validate all expected tickers are present
    expected_tickers = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'TSLA']
    actual_tickers = df['ticker'].unique().tolist()
    
    missing_tickers = set(expected_tickers) - set(actual_tickers)
    if missing_tickers:
        raise ValueError(f"Missing tickers: {missing_tickers}")
    
    print(f"✓ Data loaded successfully: {len(df)} rows")
    print(f"✓ Tickers: {sorted(actual_tickers)}")
    print(f"✓ Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    return df


if __name__ == '__main__':
    df = load_and_validate_data()
    print("\nData summary:")
    print(df.info())
    print("\nFirst few rows:")
    print(df.head())
