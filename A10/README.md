# FINM32500-A10
Market Data Storage and Querying with SQLite3 and Parquet

## Overview

This project demonstrates the storage and querying of multi-ticker OHLCV market data using both SQLite3 (relational) and Parquet (columnar) formats. It explores the tradeoffs between these formats for financial data applications including backtesting, live trading, and research.

## Project Structure

```
.
├── data_loader.py          # Data loading and validation
├── sqlite_storage.py       # SQLite3 database operations
├── parquet_storage.py      # Parquet file operations
├── schema.sql              # Database schema definition
├── market_data_multi.csv   # Source data (AAPL, AMZN, GOOG, MSFT, TSLA)
├── market_data.db          # SQLite3 database (generated)
├── market_data/            # Parquet files, partitioned by ticker (generated)
├── query_tasks.md          # Query results and performance metrics
├── comparison.md           # Format comparison and use case analysis
├── tests/                  # Unit tests
│   └── test_storage.py
└── README.md               
```

## Setup Instructions

### Requirements

- Python 3.8+
- pandas
- pyarrow
- pytest (for testing)

### Installation

```bash
# Install dependencies
pip install pandas pyarrow pytest

# Or use requirements.txt
pip install -r requirements.txt
```

### Running the Project

1. **Load and validate data:**
   ```bash
   python data_loader.py
   ```

2. **Create SQLite3 database and run queries:**
   ```bash
   python sqlite_storage.py
   ```
   This creates `market_data.db` and executes sample queries.

3. **Create Parquet files and run queries:**
   ```bash
   python parquet_storage.py
   ```
   This creates the `market_data/` directory with partitioned Parquet files.

4. **Run tests:**
   ```bash
   python -m pytest tests/ -v
   ```

## Module Descriptions

### `data_loader.py`
Handles loading and validating multi-ticker market data from CSV:
- Normalizes column names
- Converts timestamps to datetime
- Validates for missing values
- Confirms all expected tickers are present

**Key Function:**
```python
load_and_validate_data(csv_path='market_data_multi.csv') -> pd.DataFrame
```

### `sqlite_storage.py`
Manages SQLite3 database operations:
- Creates normalized schema (tickers and prices tables)
- Inserts validated data
- Executes SQL queries for various analytics:
  - Ticker/date range filtering
  - Average volume calculations
  - Return rankings
  - Daily first/last prices

**Key Functions:**
```python
create_database(db_path, schema_path)
insert_data(df, db_path)
query_ticker_date_range(ticker, start_date, end_date, db_path)
query_average_volume(db_path)
query_top_returns(db_path, limit=3)
query_first_last_daily(db_path)
```

### `parquet_storage.py`
Manages Parquet file operations:
- Converts data to columnar Parquet format
- Partitions by ticker for efficient filtering
- Performs analytical queries:
  - Rolling averages
  - Rolling volatility
  - Date range filtering by partition

**Key Functions:**
```python
save_to_parquet(df, output_dir='market_data')
load_ticker_date_range(ticker, start_date, end_date, data_dir)
compute_rolling_average(ticker, data_dir, window_minutes=5)
compute_rolling_volatility(data_dir, window=5)
```

### `schema.sql`
Defines the relational database schema:
- `tickers`: Ticker metadata (ticker_id, symbol, name, exchange)
- `prices`: OHLCV data with foreign key to tickers

### `tests/test_storage.py`
Comprehensive unit tests covering:
- Data loading and validation
- Schema creation and data insertion
- SQL query correctness
- Parquet partitioning and data integrity
- All query functions

## Key Findings

### Performance Comparison

| Metric | SQLite3 | Parquet | Winner |
|--------|---------|---------|--------|
| File Size | 672 KB | 332 KB | Parquet (2x smaller) |
| Query Speed (filtering) | ~10ms | 2.7ms | Parquet (4x faster) |
| Analytical Queries | Moderate | Fast | Parquet |
| Transactional Support | Yes | No | SQLite3 |

### Use Case Recommendations

**Use SQLite3 for:**
- Live trading systems (ACID transactions)
- Small to medium datasets
- Complex relational queries
- Incremental updates

**Use Parquet for:**
- Backtesting and research
- Large historical datasets
- Time-series analytics
- Distributed computing (Spark, Dask)

See `comparison.md` for detailed analysis.

## Data Description

**Dataset:** Multi-ticker intraday OHLCV data
- **Tickers:** AAPL, AMZN, GOOG, MSFT, TSLA
- **Time Range:** 2025-11-17 to 2025-11-21
- **Frequency:** 1-minute bars
- **Total Records:** 9,775 rows
