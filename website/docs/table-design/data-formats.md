---
title: Data Formats
sidebar_position: 6
---

In Fluss, a data format primarily defines **how data is stored and accessed**. Each format is designed to balance storage efficiency, read performance, and query capabilities.

This page describes the available formats in Fluss and provides guidance on selecting the appropriate format based on workload characteristics.

### How to Think About Formats in Fluss

At a high level, a format determines:
- How data is laid out on disk (columnar vs row-oriented)
- How efficiently data can be scanned, filtered, or projected
- Whether the workload is optimized for streaming scans or key-based access

Formats in Fluss determine:

- CPU vs IO trade-offs
- Scan-heavy vs lookup-heavy workloads
- Analytical vs operational access patterns

---

## Log Format and KV Format

In Fluss, storage formats can be used in two different ways, depending on how the data is accessed.

- **Log format** is designed for reading data in order, as it is written.
  It is commonly used for streaming workloads, append-only tables, and changelog-style data.

- **KV format** is designed for accessing data by key.
  It is used for workloads where queries look up or update values using a key and only the most recent value for each key is needed.

ARROW can be used as log format, while COMPACTED supports both log and KV formats.

## ARROW Format (Default)

### Overview

ARROW is the **default log format** in Fluss. It stores data in a columnar layout, organizing information by columns rather than rows. This layout is well suited for analytical and streaming workloads.

### Key Features

- **Column pruning**: Reads only the columns required by a query
- **Predicate pushdown**: Applies filters efficiently at the storage layer
- **Arrow ecosystem integration**: Compatible with Arrow-based processing frameworks

### When to Use ARROW

ARROW is recommended for:
- Analytical queries that access a subset of columns
- Streaming workloads with selective column reads
- General-purpose tables with varying query patterns
- Workloads that benefit from predicate pushdown

### ARROW Trade-offs

ARROW is less efficient for workloads that:
- Always read all columns
- Mostly access individual rows by key

---

## COMPACTED Format

### Overview

COMPACTED uses a **row-oriented format** that focuses on reducing storage size and CPU usage. It is optimized for workloads where queries typically access entire rows rather than individual columns.

### Key Features

- **Reduced storage overhead**: Variable-length encoding minimizes disk usage
- **Lower CPU overhead**: Efficient when all columns are accessed together
- **Row-oriented access**: Optimized for full-row reads
- **Key-value support**: Can be configured for key-based access patterns

### When to Use COMPACTED

COMPACTED is recommended for:
- Tables where queries usually select all columns
- Large vector or embedding tables
- Pre-aggregated results or materialized views
- Denormalized or joined tables
- Workloads that prioritize storage efficiency over selective column access

---

## Configuration

To enable the COMPACTED format for log data, set the `table.log.format` option:

```sql
CREATE TABLE my_table (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'table.log.format' = 'COMPACTED'
);
```

### COMPACTED with WAL Changelog Image

For key-based workloads that only require the **latest value per key**, the COMPACTED format can be used for both log and kv data, combined with the WAL changelog image mode.

```sql
CREATE TABLE kv_table (
  key STRING,
  value STRING,
  PRIMARY KEY (key) NOT ENFORCED
) WITH (
  'table.log.format' = 'COMPACTED',
  'table.kv.format' = 'COMPACTED',
  'table.changelog.image' = 'WAL'
);
```
### COMPACTED with Compression

COMPACTED log batches are not compressed by default. Set `table.log.compacted.compression.type` to `LZ4_FRAME` or `ZSTD` to compress each log batch as a whole. For `ZSTD`, the level is set with `table.log.compacted.compression.zstd.level` (1 to 22, default 3).
Compression is applied by the writer on the client side, and by the server for the changelog of primary key tables.

```sql
CREATE TABLE compressed_table (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'table.log.format' = 'COMPACTED',
  'table.log.compacted.compression.type' = 'ZSTD'
);
```

`ZSTD` with the default level 3 gives a good balance of compression ratio and CPU cost. `LZ4_FRAME` compresses less but is cheaper to compress and decompress.

:::warning
All clients reading a compressed table (e.g., Flink jobs) must be running Fluss v1.1 or later. Older clients can't detect the compression and may return a corrupted record before failing, so upgrade all consumers before creating tables with compression. The compression options can only be set when the table is created.
:::

### COMPACTED Trade-offs

COMPACTED is not recommended when:
- Queries need to read only a few columns from a table
- Filters are applied to reduce the amount of data read
- Analytical workloads require flexible access to individual columns
- Historical changes or full changelog data must be preserved

## ARROW vs COMPACTED

| Feature                | ARROW                               | COMPACTED                          |
|------------------------|-------------------------------------|------------------------------------|
| Physical layout        | Columnar                            | Row-oriented                       |
| Typical access pattern | Scans with projection & filters     | Full-row reads or key lookups      |
| Column pruning         | ✅ Yes                             | ❌ No                              |
| Predicate pushdown     | ✅ Yes                              | ❌ No                             |
| Storage efficiency     | Good                                | Excellent                          |
| Log compression        | Per column, `ZSTD` by default       | Per batch, off by default          |
| CPU efficiency         | Better for selective reads          | Better for full-row reads          |
| Log format             | ✅ Yes                              | ✅ Yes                            |
| KV format              | ❌ No                               | ✅ Yes                            |
| Best suited for        | Analytics workloads                 | State tables / materialized data   |