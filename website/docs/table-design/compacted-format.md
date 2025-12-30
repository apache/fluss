---
title: COMPACTED Table Format
---

## Overview

The COMPACTED table format is designed for key-based workloads where only the
latest value per key is required. It is supported by both Log tables and KV
tables.

In COMPACTED format, older records with the same key are compacted away,
resulting in a lightweight and efficient storage layout.

---

## Supported Table Types

- Log tables
- KV tables

---

## Configuration

The COMPACTED format can be enabled using the following table options:

```sql
CREATE TABLE kv_store (
  k STRING,
  v STRING,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'table.format' = 'COMPACTED',
  'table.changelog.image' = 'WAL'
);

---

## COMPACTED with WAL Changelog Image

When combined with `table.changelog.image = WAL`, the COMPACTED format enables
an efficient and lightweight KV store.

In this mode:

- Only the latest value per key is stored
- Previous values do not need to be looked up
- Records are not deserialized into an intermediate log format

This reduces both **latency** and **CPU overhead**, and is especially suitable
for internal and system-level tables.

---

## Use Cases

The COMPACTED format is well suited for:

- System tables
- Metadata storage
- Lookup-heavy KV workloads
- Tables that do not require full changelog reads

---

## Limitations

The COMPACTED format is not recommended when:

- Full changelog history needs to be read
- Column projections are required
- Historical state changes must be preserved