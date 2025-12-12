---
sidebar_label: Writes
title: Flink Writes
sidebar_position: 3
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

## Writing to Aggregation Tables

When using tables with the [Aggregation Merge Engine](../table-design/table-types/pk-table/merge-engines/aggregation.md), Flink provides special handling for changelog message processing.

:::warning Critical Limitation - No Exactly-Once Guarantee
**The aggregation merge engine cannot provide exactly-once semantics even when Flink is configured with exactly-once mode.** The root cause is that Fluss primary key tables do not support transactional writes and rollback mechanisms. When Flink restarts from a checkpoint, replayed records will be re-aggregated with existing values, leading to incorrect results. See [Aggregation Merge Engine Limitations](../table-design/table-types/pk-table/merge-engines/aggregation.md#limitations) for detailed explanation and recommendations.

**Root Cause:**
- Fluss primary key tables lack transaction support and rollback capabilities
- When Flink failover occurs, already-written records cannot be rolled back
- Replayed records from checkpoint will be re-aggregated, causing duplicate aggregation

**Key Points:**
- Although Flink supports exactly-once semantics, it cannot be achieved when writing to aggregation tables
- Failover will cause duplicate aggregation and incorrect results
- For exactly-once aggregation, perform aggregation in Flink state and sink final results to non-aggregation tables
:::

### Changelog Message Handling

Fluss Flink Sink processes different changelog message types when writing to aggregation tables:

| Message Type | Handling | Aggregation Behavior |
|--------------|----------|---------------------|
| `INSERT` | Written as upsert | Triggers aggregation merge with existing data |
| `UPDATE_AFTER` | Written as upsert | Triggers aggregation merge with existing data |
| `UPDATE_BEFORE` | **Ignored** | Not sent to Fluss (automatic optimization) |
| `DELETE` | Depends on configuration | See [Delete Behavior](#delete-behavior) |

:::info Automatic UPDATE_BEFORE Optimization
The Flink Sink automatically ignores `UPDATE_BEFORE` messages for aggregation tables because:
1. The aggregation merge engine does not support retraction semantics
2. `UPDATE_AFTER` already contains the complete new data for aggregation
3. This optimization reduces write operations and improves performance

This behavior is enabled by default through the `sink.ignore-delete` option, which is automatically set to `true` for aggregation tables.

**Note:** While this optimization improves performance during normal operation, it does not address the fundamental limitation of duplicate aggregation after Flink failover.
:::

### Delete Behavior

Delete operations (`DELETE` changelog messages) require explicit configuration:

```sql title="Flink SQL"
CREATE TABLE metrics (
    id BIGINT,
    count BIGINT,
    sum_value DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'aggregation',
    'table.merge-engine.aggregate.count' = 'sum',
    'table.merge-engine.aggregate.sum_value' = 'sum',
    'table.agg.remove-record-on-delete' = 'true'  -- Allow delete operations
);
```

**Configuration Options:**
- **`'table.agg.remove-record-on-delete' = 'false'`** (default): Delete operations will cause errors
- **`'table.agg.remove-record-on-delete' = 'true'`**: Delete operations will remove the entire aggregated record

:::warning Limitations
Aggregation tables have the following limitations:

**1. No Retraction Semantics**

When a `DELETE` message is received:
- The system cannot subtract values from aggregates (e.g., reverse a SUM)
- The entire record must be removed (if configured) or the operation rejected
- This is due to the current protocol design where delete operations don't carry row values

**2. No Exactly-Once Guarantee**

As explained in the [main warning above](#writing-to-aggregation-tables), the aggregation merge engine cannot provide exactly-once semantics even when Flink is configured with exactly-once mode. This is due to Fluss primary key tables lacking transaction support and rollback capabilities, which is a fundamental limitation that affects all write operations to aggregation tables.

**Recommendations:**
- Design your Flink pipeline to avoid producing `DELETE` messages for aggregation tables
- For exactly-once aggregation semantics, perform aggregation within Flink state and sink final aggregated results to tables using the `default` or `versioned` merge engine
- Only use aggregation tables for at-least-once scenarios where duplicate aggregation is acceptable
:::


### See Also

- [Aggregation Merge Engine](../table-design/table-types/pk-table/merge-engines/aggregation.md)
- [Flink Options](./options.md)
- [Flink Reads](./reads.md)