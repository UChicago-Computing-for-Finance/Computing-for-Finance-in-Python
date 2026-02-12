# Format Comparison: SQLite3 vs Parquet

## Performance Metrics

### File Size
- **SQLite3**: 672 KB (688,128 bytes)
- **Parquet**: 332 KB (339,842 bytes)
- **Winner**: Parquet is **49% smaller** due to columnar compression

### Query Speed

| Query Type | SQLite3 | Parquet | Winner |
|------------|---------|---------|--------|
| Single ticker date range | ~10ms | 2.7ms | Parquet (4x faster) |
| Aggregation (avg volume) | Fast | Fast | Similar |
| Rolling calculations | Moderate | Very Fast | Parquet |
| Complex JOINs | Fast | N/A | SQLite3 |

### Ease of Integration

**SQLite3:**
- ✓ Standard SQL interface (portable, widely known)
- ✓ ACID transactions for data integrity
- ✓ Built-in support in Python (`sqlite3` module)
- ✓ Easy to query with familiar SQL syntax
- ✗ Requires schema design and maintenance
- ✗ Less efficient for analytical queries

**Parquet:**
- ✓ Excellent compression and columnar storage
- ✓ Native integration with pandas/PyArrow
- ✓ Partition support for efficient filtering
- ✓ Ideal for big data ecosystems (Spark, Dask)
- ✗ No built-in query language (requires pandas/PyArrow)
- ✗ Not suitable for transactional operations

---

## Use Cases in Trading Systems

### When to Use SQLite3

**1. Live Trading Systems**
- **Reason**: ACID transactions ensure data consistency for order logs, trade records, and positions
- **Example**: Recording executed trades with atomic updates to positions and cash balances
- **Benefit**: Prevents data corruption during concurrent reads/writes

**2. Small to Medium Datasets (<100GB)**
- **Reason**: Single-file database is easy to backup, deploy, and manage
- **Example**: Storing daily end-of-day prices for a few hundred tickers
- **Benefit**: Simple setup, no server required

**3. Relational Queries**
- **Reason**: Complex JOINs between tickers, orders, positions, and accounts
- **Example**: Finding all open positions with their associated order history
- **Benefit**: SQL makes complex queries readable and maintainable

**4. Incremental Updates**
- **Reason**: Efficient INSERT/UPDATE for streaming data
- **Example**: Updating tick-by-tick prices in real-time
- **Benefit**: Low latency for small writes

### When to Use Parquet

**1. Backtesting and Research**
- **Reason**: Columnar format optimized for reading specific columns across many rows
- **Example**: Computing 200-day moving averages across 10 years of data for 1000 tickers
- **Benefit**: 5-10x faster than row-based formats for analytical queries

**2. Large Historical Datasets (>100GB)**
- **Reason**: Superior compression reduces storage costs by 50-80%
- **Example**: Storing tick data for multiple years
- **Benefit**: Saves disk space and speeds up I/O

**3. Distributed Computing**
- **Reason**: Native support in Spark, Dask, and cloud data warehouses
- **Example**: Parallel processing of market data across a cluster
- **Benefit**: Scales to petabyte-scale datasets

**4. Time-Series Analytics**
- **Reason**: Efficient columnar scans for aggregations and window functions
- **Example**: Computing rolling volatility, correlations, or factor exposures
- **Benefit**: Fast vectorized operations with pandas/NumPy

**5. Archival Storage**
- **Reason**: Immutable files with excellent compression
- **Example**: Long-term storage of cleaned and validated market data
- **Benefit**: Cheap to store, fast to query when needed

---

## Hybrid Approach: Best of Both Worlds

Many production systems use **both formats**:

1. **SQLite3** for operational data:
   - Current positions and orders
   - Trade execution logs
   - Real-time state management

2. **Parquet** for analytical data:
   - Historical prices for backtesting
   - Pre-computed factors and signals
   - Research datasets

**Example Workflow:**
```
Live Trading → SQLite3 (trades, positions)
     ↓
  Nightly ETL
     ↓
Parquet Archives (historical analysis)
```

---

## Summary

| Criteria | SQLite3 | Parquet |
|----------|---------|---------|
| **Storage Efficiency** | ★★★☆☆ | ★★★★★ |
| **Query Speed (Analytical)** | ★★★☆☆ | ★★★★★ |
| **Query Speed (Transactional)** | ★★★★★ | ★☆☆☆☆ |
| **Ease of Use** | ★★★★★ | ★★★★☆ |
| **Scalability** | ★★★☆☆ | ★★★★★ |
| **Data Integrity** | ★★★★★ | ★★☆☆☆ |

**Recommendation:**
- Use **SQLite3** for operational trading systems requiring transactions and relational queries
- Use **Parquet** for backtesting, research, and large-scale analytical workloads
- Consider a hybrid architecture for production systems
