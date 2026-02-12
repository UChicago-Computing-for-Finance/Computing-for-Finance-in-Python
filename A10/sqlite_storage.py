"""
SQLite3 storage module for managing multi-ticker market data.
"""
import sqlite3
import pandas as pd
from data_loader import load_and_validate_data


def create_database(db_path='market_data.db', schema_path='schema.sql'):
    """
    Create database and tables from schema file.
    
    Args:
        db_path: Path to SQLite database file
        schema_path: Path to SQL schema file
    """
    conn = sqlite3.connect(db_path)
    
    with open(schema_path, 'r') as f:
        schema = f.read()
    
    conn.executescript(schema)
    conn.commit()
    conn.close()
    print(f"✓ Database created: {db_path}")


def insert_data(df, db_path='market_data.db'):
    """
    Insert validated data into the database.
    
    Args:
        df: pandas DataFrame with validated market data
        db_path: Path to SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Insert tickers
    tickers = df['ticker'].unique()
    for idx, ticker in enumerate(tickers, start=1):
        cursor.execute(
            "INSERT OR IGNORE INTO tickers (ticker_id, symbol) VALUES (?, ?)",
            (idx, ticker)
        )
    
    # Create ticker_id mapping
    ticker_map = {ticker: idx for idx, ticker in enumerate(tickers, start=1)}
    
    # Insert prices
    for _, row in df.iterrows():
        cursor.execute(
            """INSERT INTO prices (timestamp, ticker_id, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
             ticker_map[row['ticker']],
             row['open'], row['high'], row['low'], row['close'], row['volume'])
        )
    
    conn.commit()
    conn.close()
    print(f"✓ Data inserted: {len(df)} price records")


def query_ticker_date_range(ticker, start_date, end_date, db_path='market_data.db'):
    """
    Retrieve all data for a given ticker and date range.
    
    Args:
        ticker: Ticker symbol
        start_date: Start date (string)
        end_date: End date (string)
        db_path: Path to SQLite database file
        
    Returns:
        pandas.DataFrame: Query results
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT p.timestamp, t.symbol, p.open, p.high, p.low, p.close, p.volume
    FROM prices p
    JOIN tickers t ON p.ticker_id = t.ticker_id
    WHERE t.symbol = ? AND p.timestamp BETWEEN ? AND ?
    ORDER BY p.timestamp
    """
    
    df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))
    conn.close()
    
    return df


def query_average_volume(db_path='market_data.db'):
    """
    Calculate average daily volume per ticker.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        pandas.DataFrame: Query results
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT t.symbol, AVG(p.volume) as avg_volume
    FROM prices p
    JOIN tickers t ON p.ticker_id = t.ticker_id
    GROUP BY t.symbol
    ORDER BY avg_volume DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


def query_top_returns(db_path='market_data.db', limit=3):
    """
    Identify top tickers by return over the full period.
    
    Args:
        db_path: Path to SQLite database file
        limit: Number of top tickers to return
        
    Returns:
        pandas.DataFrame: Query results
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT t.symbol,
           MIN(p.timestamp) as first_time,
           MAX(p.timestamp) as last_time,
           (SELECT close FROM prices WHERE ticker_id = t.ticker_id ORDER BY timestamp LIMIT 1) as first_close,
           (SELECT close FROM prices WHERE ticker_id = t.ticker_id ORDER BY timestamp DESC LIMIT 1) as last_close,
           ((SELECT close FROM prices WHERE ticker_id = t.ticker_id ORDER BY timestamp DESC LIMIT 1) - 
            (SELECT close FROM prices WHERE ticker_id = t.ticker_id ORDER BY timestamp LIMIT 1)) / 
            (SELECT close FROM prices WHERE ticker_id = t.ticker_id ORDER BY timestamp LIMIT 1) * 100 as return_pct
    FROM tickers t
    JOIN prices p ON t.ticker_id = p.ticker_id
    GROUP BY t.symbol
    ORDER BY return_pct DESC
    LIMIT ?
    """
    
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    
    return df


def query_first_last_daily(db_path='market_data.db'):
    """
    Find first and last trade price for each ticker per day.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        pandas.DataFrame: Query results
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT t.symbol,
           DATE(p.timestamp) as date,
           MIN(p.timestamp) as first_time,
           MAX(p.timestamp) as last_time,
           (SELECT close FROM prices 
            WHERE ticker_id = t.ticker_id AND DATE(timestamp) = DATE(p.timestamp)
            ORDER BY timestamp LIMIT 1) as first_price,
           (SELECT close FROM prices 
            WHERE ticker_id = t.ticker_id AND DATE(timestamp) = DATE(p.timestamp)
            ORDER BY timestamp DESC LIMIT 1) as last_price
    FROM prices p
    JOIN tickers t ON p.ticker_id = t.ticker_id
    GROUP BY t.symbol, DATE(p.timestamp)
    ORDER BY date, t.symbol
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


if __name__ == '__main__':
    # Load and validate data
    df = load_and_validate_data()
    
    # Create database
    create_database()
    
    # Insert data
    insert_data(df)
    
    # Run sample queries
    print("\n" + "="*60)
    print("Query 1: TSLA data from 2025-11-17 to 2025-11-18")
    print("="*60)
    result = query_ticker_date_range('TSLA', '2025-11-17', '2025-11-18')
    print(result.head(10))
    
    print("\n" + "="*60)
    print("Query 2: Average daily volume per ticker")
    print("="*60)
    result = query_average_volume()
    print(result)
    
    print("\n" + "="*60)
    print("Query 3: Top 3 tickers by return")
    print("="*60)
    result = query_top_returns()
    print(result)
    
    print("\n" + "="*60)
    print("Query 4: First and last price per day (sample)")
    print("="*60)
    result = query_first_last_daily()
    print(result.head(10))
