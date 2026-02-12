"""
Unit tests for market data storage and querying system.
"""
import unittest
import pandas as pd
import sqlite3
import os
from pathlib import Path
import shutil

from data_loader import load_and_validate_data
from sqlite_storage import (
    create_database, insert_data, query_ticker_date_range,
    query_average_volume, query_top_returns, query_first_last_daily
)
from parquet_storage import (
    save_to_parquet, load_ticker_date_range as load_parquet_ticker_date_range,
    compute_rolling_volatility, compute_rolling_average
)


class TestDataLoader(unittest.TestCase):
    """Test data loading and validation."""
    
    def test_load_data(self):
        """Test data loading from CSV."""
        df = load_and_validate_data()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
    
    def test_validate_columns(self):
        """Test that all required columns are present."""
        df = load_and_validate_data()
        required_cols = ['timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            self.assertIn(col, df.columns)
    
    def test_validate_tickers(self):
        """Test that all expected tickers are present."""
        df = load_and_validate_data()
        expected_tickers = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'TSLA']
        actual_tickers = sorted(df['ticker'].unique().tolist())
        self.assertEqual(actual_tickers, expected_tickers)
    
    def test_no_missing_values(self):
        """Test that there are no missing values in key columns."""
        df = load_and_validate_data()
        self.assertEqual(df['timestamp'].isnull().sum(), 0)
        self.assertEqual(df['close'].isnull().sum(), 0)


class TestSQLiteStorage(unittest.TestCase):
    """Test SQLite3 storage and querying."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database."""
        cls.test_db = 'test_market_data.db'
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
        
        # Create database and insert data
        df = load_and_validate_data()
        create_database(cls.test_db, 'schema.sql')
        insert_data(df, cls.test_db)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test database."""
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
    
    def test_schema_creation(self):
        """Test that tables are created correctly."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        
        # Check tickers table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tickers'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check prices table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prices'")
        self.assertIsNotNone(cursor.fetchone())
        
        conn.close()
    
    def test_data_insertion(self):
        """Test that data is inserted correctly."""
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        
        # Check tickers count
        cursor.execute("SELECT COUNT(*) FROM tickers")
        ticker_count = cursor.fetchone()[0]
        self.assertEqual(ticker_count, 5)
        
        # Check prices count
        cursor.execute("SELECT COUNT(*) FROM prices")
        price_count = cursor.fetchone()[0]
        self.assertGreater(price_count, 0)
        
        conn.close()
    
    def test_query_ticker_date_range(self):
        """Test ticker date range query."""
        result = query_ticker_date_range('AAPL', '2025-11-17', '2025-11-18', self.test_db)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        self.assertTrue((result['symbol'] == 'AAPL').all())
    
    def test_query_average_volume(self):
        """Test average volume query."""
        result = query_average_volume(self.test_db)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)
        self.assertIn('avg_volume', result.columns)
    
    def test_query_top_returns(self):
        """Test top returns query."""
        result = query_top_returns(self.test_db, limit=3)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertIn('return_pct', result.columns)
    
    def test_query_first_last_daily(self):
        """Test first/last daily price query."""
        result = query_first_last_daily(self.test_db)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        self.assertIn('first_price', result.columns)
        self.assertIn('last_price', result.columns)


class TestParquetStorage(unittest.TestCase):
    """Test Parquet storage and querying."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test Parquet files."""
        cls.test_dir = 'test_market_data'
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)
        
        # Create Parquet files
        df = load_and_validate_data()
        save_to_parquet(df, cls.test_dir)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test Parquet files."""
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)
    
    def test_parquet_creation(self):
        """Test that Parquet files are created."""
        self.assertTrue(os.path.exists(self.test_dir))
        self.assertTrue(os.path.isdir(self.test_dir))
        
        # Check for ticker partitions
        partitions = [d.name for d in Path(self.test_dir).iterdir() if d.is_dir()]
        self.assertGreater(len(partitions), 0)
    
    def test_parquet_partitioning(self):
        """Test that data is partitioned by ticker."""
        partitions = [d.name for d in Path(self.test_dir).iterdir() if d.is_dir()]
        expected_partitions = ['ticker=AAPL', 'ticker=AMZN', 'ticker=GOOG', 
                               'ticker=MSFT', 'ticker=TSLA']
        
        for partition in expected_partitions:
            self.assertIn(partition, partitions)
    
    def test_load_ticker_date_range(self):
        """Test loading ticker data from Parquet."""
        result = load_parquet_ticker_date_range('TSLA', '2025-11-17', '2025-11-18', self.test_dir)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        self.assertTrue((result['ticker'] == 'TSLA').all())
    
    def test_compute_rolling_average(self):
        """Test rolling average computation."""
        result = compute_rolling_average('AAPL', self.test_dir, window_minutes=5)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('rolling_avg', result.columns)
        self.assertGreater(len(result), 0)
    
    def test_compute_rolling_volatility(self):
        """Test rolling volatility computation."""
        result = compute_rolling_volatility(self.test_dir, window=5)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('rolling_volatility', result.columns)
        self.assertGreater(len(result), 0)
    
    def test_data_integrity(self):
        """Test that data loaded from Parquet matches original."""
        original_df = load_and_validate_data()
        parquet_df = pd.read_parquet(self.test_dir)
        
        # Compare row counts
        self.assertEqual(len(original_df), len(parquet_df))
        
        # Compare tickers
        original_tickers = sorted(original_df['ticker'].unique())
        parquet_tickers = sorted(parquet_df['ticker'].unique())
        self.assertEqual(original_tickers, parquet_tickers)


if __name__ == '__main__':
    unittest.main()
