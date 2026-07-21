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

## Failover Recovery for Aggregation Tables

For tables that use the aggregation merge engine, the Flink connector automatically selects the
failover recovery behavior from the aggregation functions and the records that the sink can emit.
No additional connector option is required.

The connector skips undo recovery when every column written by the job uses an idempotent
aggregation function and deletes cannot change the materialized state. Applying the same
aggregation input again does not change the materialized result for these functions:

- `max` and `min`
- `first_value` and `first_value_ignore_nulls`
- `last_value` and `last_value_ignore_nulls`
- `bool_and` and `bool_or`
- `rbm32` and `rbm64`

Undo recovery remains enabled for non-idempotent functions such as `sum`, `product`, `listagg`,
and `string_agg`. It is also retained when the input can contain an effective delete or when the
connector cannot prove that skipping recovery is safe. For partial updates, only the columns
written by the job participate in this decision.

When undo recovery is skipped, the connector does not track bucket offsets or apply data undo.
When `sink.producer-id` is configured, subtask 0 removes stale producer offsets for that ID before
records are processed; the configured ID must not be shared by concurrently running sinks. The
default Flink job ID is not used for this cleanup because all sinks in a job share it; a fresh job
submission naturally receives a new ID. Other subtasks do not connect for recovery.

:::note Checkpoint compatibility
Markerless checkpoints from older connector versions use the recovery action selected by the
current job. A checkpoint with undo recovery skipped clears old undo state and cannot later be
restored by a job revision that requires undo recovery, because it contains no bucket-offset
baseline. Keep the same recovery-safe write semantics when restoring such a checkpoint, or start
the changed job without restoring it.
:::

## DELETE FROM

Fluss supports deleting data for primary-key tables in batch mode via `DELETE FROM` statement. Currently, only single data deletions based on the primary key are supported.

* the Primary Key Table
```sql title="Flink SQL"
-- DELETE statement requires batch mode
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
-- The condition must include all primary key equality conditions.
DELETE FROM pk_table WHERE shop_id = 10000 AND user_id = 123456;
```

## UPDATE
Fluss enables data updates for primary-key tables in batch mode using the `UPDATE` statement. Currently, only single-row updates based on the primary key are supported.

```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

```sql title="Flink SQL"
-- The condition must include all primary key equality conditions.
UPDATE pk_table SET total_amount = 2 WHERE shop_id = 10000 AND user_id = 123456;
```
