"""
Parquet storage module for managing multi-ticker market data.
"""
import pandas as pd
import time
from pathlib import Path
from data_loader import load_and_validate_data


def save_to_parquet(df, output_dir='market_data'):
    """
    Convert dataset to Parquet format, partitioned by ticker.
    
    Args:
        df: pandas DataFrame with market data
        output_dir: Directory to save Parquet files
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Save partitioned by ticker
    df.to_parquet(
        output_path,
        engine='pyarrow',
        partition_cols=['ticker'],
        index=False
    )
    
    print(f"✓ Data saved to Parquet: {output_dir}/")
    
    # Display file structure
    for ticker_dir in sorted(output_path.iterdir()):
        if ticker_dir.is_dir():
            print(f"  {ticker_dir.name}/")


def load_ticker_date_range(ticker, start_date, end_date, data_dir='market_data'):
    """
    Retrieve all data for a given ticker and date range from Parquet.
    
    Args:
        ticker: Ticker symbol
        start_date: Start date (string)
        end_date: End date (string)
        data_dir: Directory containing Parquet files
        
    Returns:
        pandas.DataFrame: Query results
    """
    df = pd.read_parquet(
        data_dir,
        filters=[('ticker', '=', ticker)]
    )
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Filter by date range
    mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
    result = df[mask].sort_values('timestamp')
    
    return result


def compute_rolling_volatility(data_dir='market_data', window=5):
    """
    Compute rolling N-day volatility (standard deviation of returns) for each ticker.
    
    Args:
        data_dir: Directory containing Parquet files
        window: Rolling window size in days
        
    Returns:
        pandas.DataFrame: Ticker, date, and rolling volatility
    """
    df = pd.read_parquet(data_dir)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Calculate returns
    df = df.sort_values(['ticker', 'timestamp'])
    df['returns'] = df.groupby('ticker')['close'].pct_change()
    
    # Add date column
    df['date'] = df['timestamp'].dt.date
    
    # Calculate daily returns (last return of each day)
    daily_returns = df.groupby(['ticker', 'date'])['returns'].last().reset_index()
    
    # Calculate rolling volatility
    volatility = daily_returns.groupby('ticker')['returns'].rolling(
        window=window, min_periods=window
    ).std().reset_index(drop=True)
    
    daily_returns['rolling_volatility'] = volatility
    
    # Drop NaN values
    result = daily_returns.dropna()
    
    return result


def compute_rolling_average(ticker, data_dir='market_data', window_minutes=5):
    """
    Load all data for a ticker and compute rolling average of close price.
    
    Args:
        ticker: Ticker symbol
        data_dir: Directory containing Parquet files
        window_minutes: Rolling window size in minutes
        
    Returns:
        pandas.DataFrame: Data with rolling average
    """
    df = pd.read_parquet(
        data_dir,
        filters=[('ticker', '=', ticker)]
    )
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # Calculate rolling average
    df['rolling_avg'] = df['close'].rolling(window=window_minutes).mean()
    
    return df


def get_file_size(path):
    """
    Get total size of directory or file in bytes.
    
    Args:
        path: Path to file or directory
        
    Returns:
        int: Total size in bytes
    """
    path = Path(path)
    
    if path.is_file():
        return path.stat().st_size
    elif path.is_dir():
        return sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
    
    return 0


if __name__ == '__main__':
    # Load and validate data
    df = load_and_validate_data()
    
    # Save to Parquet
    save_to_parquet(df)
    
    # Query 1: Load AAPL data and compute rolling average
    print("\n" + "="*60)
    print("Query 1: AAPL with 5-minute rolling average")
    print("="*60)
    result = compute_rolling_average('AAPL')
    print(result[['timestamp', 'close', 'rolling_avg']].head(10))
    
    # Query 2: Compute 5-day rolling volatility
    print("\n" + "="*60)
    print("Query 2: 5-day rolling volatility for all tickers")
    print("="*60)
    result = compute_rolling_volatility()
    print(result.head(20))
    
    # Query 3: Compare performance with SQLite3
    print("\n" + "="*60)
    print("Query 3: Performance comparison - TSLA 2025-11-17 to 2025-11-18")
    print("="*60)
    
    # Parquet timing
    start = time.time()
    parquet_result = load_ticker_date_range('TSLA', '2025-11-17', '2025-11-18')
    parquet_time = time.time() - start
    
    print(f"Parquet query time: {parquet_time:.4f} seconds")
    print(f"Rows returned: {len(parquet_result)}")
    
    # File sizes
    parquet_size = get_file_size('market_data')
    try:
        sqlite_size = get_file_size('market_data.db')
        print(f"\nParquet size: {parquet_size:,} bytes ({parquet_size/1024:.2f} KB)")
        print(f"SQLite size: {sqlite_size:,} bytes ({sqlite_size/1024:.2f} KB)")
        print(f"Size ratio (Parquet/SQLite): {parquet_size/sqlite_size:.2f}x")
    except:
        print(f"\nParquet size: {parquet_size:,} bytes ({parquet_size/1024:.2f} KB)")
        print("SQLite database not found for comparison")
