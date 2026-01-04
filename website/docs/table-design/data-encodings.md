---
title: Data Encodings
---

## Overview

Fluss supports multiple **data encodings** to optimize how data is stored and read for different workloads. Each encoding is designed to
balance storage efficiency, read performance, and query capabilities.

This page describes the available encodings in Fluss and provides guidance on selecting the appropriate encoding based on workload characteristics.

---

## Log Encoding and KV Encoding

In Fluss, data encodings can be used in two different ways, depending on how the data is accessed.

- **Log encoding** is designed for reading data in order, as it is written.
  It is commonly used for streaming workloads, append-only tables, and changelog-style data.

- **KV encoding** is designed for accessing data by key.
  It is used for workloads where queries look up or update values using a key and only the most recent value for each key is needed.

ARROW can be used as log encoding, while COMPACTED supports both.

## ARROW Encoding (Default)

### Overview

ARROW is the **default encoding** in Fluss. It stores data in a columnar layout, organizing information by columns rather than rows. This layout is well suited for analytical and streaming workloads.

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
- Workloads that mostly access individual rows by key

---

## COMPACTED Encoding

### Overview

COMPACTED uses a **row-oriented encoding** that focuses on reducing storage size and CPU usage. It is optimized for workloads where queries typically access entire rows rather than individual columns.

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

To enable the COMPACTED encoding, set the `table.format` option:

```sql
CREATE TABLE my_table (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'table.format' = 'COMPACTED'
);
```

### COMPACTED with WAL Changelog Image

For key-based workloads that only require the **latest value per key**, the COMPACTED encoding can be combined with the WAL changelog image mode.

```sql
CREATE TABLE kv_table (
  key STRING,
  value STRING,
  PRIMARY KEY (key) NOT ENFORCED
) WITH (
  'table.format' = 'COMPACTED',
  'table.changelog.image' = 'WAL'
);
```
### COMPACTED Trade-offs

COMPACTED is not recommended when:
- Queries need to read only a few columns from a table
- Filters are applied to reduce the amount of data read
- Analytical workloads require flexible access to individual columns
- Historical changes or full changelog data must be preserved
