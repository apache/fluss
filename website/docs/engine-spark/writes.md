---
sidebar_label: Writes
title: Spark Writes
sidebar_position: 4
---

# Spark Writes

You can directly insert data into a Fluss table using the `INSERT INTO` statement.
Fluss primary key tables support upsert semantics (inserting a row with an existing primary key will update the existing record), while Fluss log tables only accept append-only writes.

## INSERT INTO

`INSERT INTO` statements are used to write data to Fluss tables in batch mode.
They are compatible with primary-key tables (for upserting data) as well as log tables (for appending data).

### Appending Data to the Log Table

#### Create a Log Table

```sql title="Spark SQL"
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
);
```

#### Insert Data into the Log Table

```sql title="Spark SQL"
INSERT INTO log_table VALUES
  (600, 21, 601, 'addr1'),
  (700, 22, 602, 'addr2'),
  (800, 23, 603, 'addr3'),
  (900, 24, 604, 'addr4'),
  (1000, 25, 605, 'addr5');
```

### Perform Data Upserts to the PrimaryKey Table

#### Create a primary key table

```sql title="Spark SQL"
CREATE TABLE pk_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) TBLPROPERTIES (
  'primary.key' = 'order_id',
  'bucket.num' = '1'
);
```

#### Insert Data

```sql title="Spark SQL"
INSERT INTO pk_table VALUES
  (600, 21, 601, 'addr1'),
  (700, 22, 602, 'addr2'),
  (800, 23, 603, 'addr3');
```

#### Upsert Data

When inserting data with the same primary key, the existing record will be updated:

```sql title="Spark SQL"
-- This will update the records with order_id 700 and 800
INSERT INTO pk_table VALUES
  (700, 220, 602, 'addr2'),
  (800, 230, 603, 'addr3');
```

### All Data Types

Fluss Spark connector supports all Fluss data types including nested types. Here is an example of writing various data types:

:::note
The `MAP` type is currently **not supported** for write operations. Full MAP type write support will be available soon.
:::

```sql title="Spark SQL"
CREATE TABLE all_types_table (
  bool_col BOOLEAN,
  tinyint_col BYTE,
  smallint_col SHORT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  decimal_col DECIMAL(10, 2),
  decimal_large_col DECIMAL(38, 2),
  string_col STRING,
  ts_col TIMESTAMP,
  array_col ARRAY<FLOAT>,
  struct_col STRUCT<id: BIGINT, name: STRING>
);
```

```sql title="Spark SQL"
INSERT INTO all_types_table VALUES (
  true, 1, 10, 100, 1000, 12.3, 45.6,
  1234567.89, 12345678900987654321.12,
  'test',
  TIMESTAMP '2025-12-31 10:00:00',
  array(11.11, 22.22), struct(123, 'apache fluss')
);
```

## See Also

- [Structured Streaming Write](streaming.md#streaming-write) for continuous streaming writes to Fluss tables.
