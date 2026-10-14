---
sidebar_label: Writes
title: Flink Writes
sidebar_position: 4
---

# Flink Writes

You can directly insert or update data into a Fluss table using the `INSERT INTO` statement.
Fluss primary key tables can accept all types of messages (`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`), while Fluss log table can only accept `INSERT` type messages.


## INSERT INTO
`INSERT INTO` statements are used to write data to Fluss tables. 
They support both streaming and batch modes and are compatible with primary-key tables (for upserting data) as well as log tables (for appending data).

### Appending Data to the Log Table
#### Create a Log Table.
```sql title="Flink SQL"
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
);
```

#### Insert Data into the Log Table.
```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
INSERT INTO log_table
SELECT * FROM source;
```


### Perform Data Upserts to the PrimaryKey Table.

#### Create a primary key table.
```sql title="Flink SQL"
CREATE TABLE pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
);
```

#### Updates All Columns
```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
INSERT INTO pk_table
SELECT * FROM source;
```


#### Partial Updates

```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
-- only partial-update the num_orders column
INSERT INTO pk_table (shop_id, user_id, num_orders)
SELECT shop_id, user_id, num_orders FROM source;
```

## DELETE FROM

Fluss supports deleting data for primary-key tables in batch mode via `DELETE FROM` statement. The `WHERE` clause can be any condition and does not need to cover all primary key columns.

:::note
`DELETE FROM` and `UPDATE` are only supported for primary-key tables using the default merge engine. Tables with the `first_row`, `versioned`, or `aggregation` merge engine reject both statements.
:::

```sql title="Flink SQL"
-- DELETE statement requires batch mode
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
-- Delete a single row by primary key
DELETE FROM pk_table WHERE shop_id = 10000 AND user_id = 123456;

-- Delete rows by a non-primary-key condition
DELETE FROM pk_table WHERE total_amount > 100;
```

If the `WHERE` clause consists only of equality predicates on every primary key column, the row is deleted directly by primary key. Otherwise, Fluss scans the table (pruned by any partition column predicates) and deletes all matching rows.

:::note
`DELETE FROM` respects the table's `table.delete.behavior` option: `disable` rejects the statement, and `ignore` succeeds without deleting any rows.
:::

## UPDATE
Fluss enables data updates for primary-key tables in batch mode using the `UPDATE` statement. The `WHERE` clause can be any condition and does not need to cover all primary key columns.

```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

```sql title="Flink SQL"
-- Update a single row by primary key
UPDATE pk_table SET total_amount = 2 WHERE shop_id = 10000 AND user_id = 123456;

-- Update rows by a non-primary-key condition
UPDATE pk_table SET total_amount = 0 WHERE num_orders = 0;
```

If the `WHERE` clause consists only of equality predicates on every primary key column, the row is read by a single primary key lookup. Otherwise, Fluss scans the table (pruned by any partition column predicates) and updates all matching rows.

:::note
Primary key columns cannot be updated. For partitioned primary-key tables this includes the partition columns, which are always part of the primary key.
:::

:::caution
`UPDATE` and `DELETE FROM` are not atomic. Each statement reads the table once and then writes matching rows independently: rows written after the read starts are not affected, concurrent writes to a matching row may be overwritten (`UPDATE` writes back the entire row), and a failed job leaves already-written rows modified. Avoid running these statements concurrently with other writers to the same keys.
:::
