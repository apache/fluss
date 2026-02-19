---
title: "Union Read"
sidebar_position: 3
---

# Union Read

Union Read is a core feature of Fluss's Streaming Lakehouse that combines real-time data from Fluss with historical data from the data lake in a single query.

## Overview

For a table with `'table.datalake.enabled' = 'true'`, data exists in two layers:
- **Fluss (hot data)**: Sub-second fresh data stored in Arrow format
- **Data Lake (cold data)**: Historical data stored in Parquet format

Union Read transparently merges data from both sources, providing sub-second freshness with full historical coverage.

## Querying Tables

### Union Read (Default)

Query the table directly to read combined Fluss and lake data:

```sql title="Flink SQL"
-- Union Read: combines real-time Fluss data + historical lake data
SELECT * FROM my_table;

-- Aggregations work across both data sources
SELECT COUNT(*), SUM(amount) FROM orders;
```

### Lake-Only Read

To query only the data stored in the data lake, use the `$lake` suffix:

```sql title="Flink SQL"
-- Lake-only read: queries only tiered data
SELECT * FROM my_table$lake;

-- Access lake-specific system tables
SELECT snapshot_id, total_record_count FROM my_table$lake$snapshots;
```

Lake-only queries are useful when:
- Real-time freshness is not required
- You need to access lake format-specific system tables
- You want optimized performance for large historical scans

## Execution Modes

Union Read supports both batch and streaming modes:

### Batch Mode

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'batch';
SELECT SUM(total_price) FROM orders;
```

The query merges rows from both lake and Fluss, returning the most up-to-date results. Multiple executions may produce different outputs as data is continuously ingested.

### Streaming Mode

```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM orders;
```

Flink first reads the latest lake snapshot, then switches to Fluss starting from the log offset aligned with that snapshot, ensuring exactly-once semantics.

## Data Deduplication

For **primary key tables**, Union Read automatically deduplicates records with the same key, keeping the latest version from Fluss if it exists.

For **log tables** (append-only), Union Read concatenates data from both sources without deduplication.

## Data Freshness

The `table.datalake.freshness` option controls how often data is tiered to the lake:

```sql title="Flink SQL"
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    amount DECIMAL(15, 2)
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '1min'
);
```

- **Shorter freshness** (e.g., `30s`): Lake data stays closer to real-time, less data read from Fluss
- **Longer freshness** (e.g., `10min`): Better lake read performance due to larger files, more data read from Fluss

## Data Retention

Key behavior for data retention with Union Read:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via the lake if previously tiered
- **Cleaned-up partitions** in partitioned tables (controlled by `table.auto-partition.num-retention`) remain accessible via the lake if previously tiered

## Engine Support

| Engine | Union Read | Lake-Only Read |
|--------|------------|----------------|
| Apache Flink | ✅ | ✅ Via `$lake` suffix |
| Apache Spark | ❌ | ✅ Via native lake connectors |
| Trino | ❌ | ✅ Via native lake connectors |
| StarRocks | ❌ | ✅ Via native lake connectors |

External engines can access the tiered lake data directly through native lake format connectors. See the specific data lake format documentation for examples:
- [Paimon - Reading with other Engines](datalake-formats/paimon.md#reading-with-other-engines)
- [Iceberg - Reading with other Engines](datalake-formats/iceberg.md#reading-with-other-engines)
