---
title: Data Encodings
---

## Overview

Fluss supports multiple **data encodings** to optimize storage layout and access
patterns for different workloads. Each encoding represents a set of trade-offs
between storage efficiency, read performance, and supported access patterns.

Encodings can be applied to different table types, such as **Log tables** and
**KV tables**, depending on the encoding’s capabilities and design goals.

This page provides an overview of the encoding landscape in Fluss and guidance
on when to choose a particular encoding.

---

## Log Encoding vs KV Encoding

Encodings in Fluss can be used in two different contexts:

- **Log encoding** focuses on efficient sequential reads and streaming
  consumption. It is commonly used for append-only or changelog-style access
  patterns.
- **KV encoding** optimizes for key-based access patterns, such as point lookups
  and updates, where only the latest value per key is relevant.

Not all encodings support both table types. The following sections describe the
supported encodings and their applicable use cases.

---

## ARROW Encoding

### Characteristics

ARROW is a **columnar encoding** designed for analytical and streaming workloads.
It can **only be used as a log encoding**.

By storing data in a columnar layout, ARROW enables efficient column access and
better CPU cache utilization. It also integrates well with Arrow-based systems.

### Primary Use Cases

- Streaming analytical workloads
- Queries that benefit from column projection
- Integration with Arrow-based processing frameworks

### Trade-offs

- Not supported for KV tables
- Not optimized for key-based point lookups

---    

## COMPACTED Encoding

### Characteristics

COMPACTED is a **row-oriented encoding** that leverages variable-length coding to
minimize binary data size. It is designed for key-based workloads where only the
**latest value per key** is required.

COMPACTED can be used as both a **log encoding** and a **KV encoding**.

### Supported Table Types

- Log tables
- KV tables

### Configuration

The COMPACTED encoding can be enabled using the following table options:

```sql
CREATE TABLE kv_store (
  k STRING,
  v STRING,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'table.format' = 'COMPACTED',
  'table.changelog.image' = 'WAL'
);

### COMPACTED with WAL Changelog Image

When combined with `table.changelog.image = WAL`, the COMPACTED encoding enables
an efficient and lightweight KV store optimized for key-based access patterns.

In this mode:

- Only the latest value for each key is stored
- Previous values do not need to be looked up
- Records are not deserialized into an intermediate log format

As a result, this approach reduces both **latency** and **CPU overhead**, making
it especially suitable for internal or system-level tables.

### Primary Use Cases:

- System tables
- Internal metadata storage
- Lookup-heavy KV workloads
- Use cases that do not require full changelog reads

### Trade-offs and Limitations

The COMPACTED encoding is not recommended when:

- Full changelog history needs to be read
- Column projection is required
- Historical state changes must be preserved

## INDEXED Encoding (Deprecated)

### Characteristics

INDEXED is a **row-oriented encoding** that supports both log and KV tables.
However, it is **deprecated** and should not be used for new applications.

### Recommendation

Existing users may continue to rely on INDEXED, but new tables should prefer
COMPACTED or ARROW, depending on workload characteristics.
